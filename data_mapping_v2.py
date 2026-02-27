import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from config import OUTPUT_DIR, RAW_TD_DIR, RAW_OPER_TIME_FILE, RAW_QUALITY_FILE, PROCEDURE_FILES, PROCESS_COLUMNS_CONFIG


# ====================== 增强版日志系统 ======================
def log_event(level, msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{level} {timestamp}] {msg}")


ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]
TIME_PARSE_KWARGS = {"errors": "coerce"}
BATCH_SIZE = 500


# ====================== 1. StatsFactory：物理特性建模 ======================
class StatsFactory:
    def __init__(self, raw_dir, config):
        self.raw_dir = raw_dir
        self.config = config

    def analyze(self) -> Dict:
        log_event("STEP", "开始阶段 0：分析传感器物理分布 (A/B类补全基础)")
        stats_lookup = {}
        for file_prefix, cfg in self.config.items():
            alias = cfg['alias']
            files = glob.glob(os.path.join(self.raw_dir, f"{file_prefix}*.csv"))
            if not files: continue

            all_vals = []
            for f in files[:5]:  # 采样前5个文件
                try:
                    df = pd.read_csv(f, usecols=['val'])
                    all_vals.append(df['val'].dropna())
                except:
                    continue

            if not all_vals: continue
            combined = pd.concat(all_vals)

            # 物理判定逻辑
            force_zero_keywords = ['SPEED', 'FORCE', 'PRESS', 'RED', 'THICK']
            is_mechanical = any(k in alias for k in force_zero_keywords)

            if is_mechanical:
                ambient = 0.0
                # 统计工作态：排除掉处于极小值的数据
                work_data = combined[combined.abs() > (combined.max() * 0.05)]
                work_mean = work_data.mean() if not work_data.empty else combined.mean()
                work_std = work_data.std() if not work_data.empty else 0.1
            else:
                # 温度类：P05作为底色，保底20度
                ambient = max(combined.quantile(0.05), 20.0)
                work_mean = combined.mean()
                work_std = combined.std()

            stats_lookup[alias] = {
                "ambient": ambient,
                "work_mean": work_mean,
                "work_std": work_std if work_std > 0 else 0.1,
                "is_mechanical": is_mechanical
            }
        log_event("INFO", f"建模完成，已成功分析 {len(stats_lookup)} 个特征维度的分布特性。")
        return stats_lookup


# ====================== 2. 物理补全与 Mask 向量化处理 ======================
def impute_with_physics(df_batch: pd.DataFrame, stats_lookup: Dict):
    mask_dict = {}
    stats_a, stats_b = 0, 0

    for col in ALL_ALIASES:
        if col not in df_batch.columns: continue

        # 1. 识别真实观测点
        is_real = (df_batch[col] != 0) & (df_batch[col].notnull())
        mask_dict[f"{col}_MSK"] = is_real.astype('uint8')

        stat = stats_lookup.get(col)
        if not stat: continue

        proc_prefix = col.split('_')[0]
        in_work_zone = df_batch['PROCEDURE_NAME'].str.contains(proc_prefix, na=False)

        # 2. A类填充：逻辑在工序内但缺失 (该有没数)
        mask_a = (~is_real) & in_work_zone
        if mask_a.any():
            # 使用基于标准差的正态分布采样，模拟真实的物理抖动
            sampled_vals = np.random.normal(stat['work_mean'], stat['work_std'] * 0.4, size=mask_a.sum())
            # 限制采样范围在合理区间，防止极端离群值
            df_batch.loc[mask_a, col] = np.clip(sampled_vals, stat['work_mean'] * 0.4, stat['work_mean'] * 1.6)
            stats_a += mask_a.sum()

        # 3. B类填充：逻辑不在工序内 (背景底色)
        mask_b = (~is_real) & (~in_work_zone)
        if mask_b.any():
            df_batch.loc[mask_b, col] = stat['ambient']
            stats_b += mask_b.sum()

    # 一次性合并 Mask，解决碎片化警告并提升性能
    if mask_dict:
        df_masks = pd.DataFrame(mask_dict, index=df_batch.index)
        df_batch = pd.concat([df_batch, df_masks], axis=1)

    return df_batch, stats_a, stats_b


# ====================== 3. 基础处理工具函数 ======================
def get_file_config(filename: str) -> Optional[Dict]:
    base_name = os.path.splitext(filename)[0]
    parts = base_name.split('_')
    for i in range(len(parts), 0, -1):
        prefix = '_'.join(parts[:i])
        if prefix in PROCESS_COLUMNS_CONFIG: return PROCESS_COLUMNS_CONFIG[prefix]
    return None


def discover_csv_files():
    discovered = {}
    for proc, patterns in PROCEDURE_FILES.items():
        actual = []
        for p in patterns:
            matching = glob.glob(os.path.join(RAW_TD_DIR, p))
            actual.extend([os.path.basename(f) for f in matching if f.endswith('.csv')])
        discovered[proc] = sorted(list(set(actual)))
    return discovered


def load_base_tables():
    log_event("STEP", "正在载入 Excel 基础配置表...")
    df_q = pd.read_excel(RAW_QUALITY_FILE)
    df_o = pd.read_excel(RAW_OPER_TIME_FILE)
    if "SLAB_ID" not in df_q.columns:
        df_q = df_q.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
    df_o["START_TIME"] = pd.to_datetime(df_o["START_TIME"], **TIME_PARSE_KWARGS)
    df_o["END_TIME"] = pd.to_datetime(df_o["END_TIME"], **TIME_PARSE_KWARGS)
    return df_q, df_o


def load_process_data(filename):
    cfg = get_file_config(filename)
    df = pd.read_csv(os.path.join(RAW_TD_DIR, filename))
    t_col = cfg["time_col"]
    df[t_col] = pd.to_datetime(df[t_col], **TIME_PARSE_KWARGS).dt.floor("1s")
    return df.dropna(subset=[t_col]).sort_values(t_col)


def extract_timesteps_by_time_window(df_proc, df_oper_proc, file_cfg, procedure, alias):
    t_col = file_cfg["time_col"]
    val_col = next(v for v in file_cfg["value_cols"] if v in df_proc.columns)
    times, starts, ends, sids = df_proc[t_col].values, df_oper_proc["START_TIME"].values, df_oper_proc[
        "END_TIME"].values, df_oper_proc["SLAB_ID"].values

    res = []
    for s, e, sid in zip(starts, ends, sids):
        l, r = np.searchsorted(times, [s, e])
        if l >= r: continue
        win = df_proc.iloc[l:r].copy()
        win = win.rename(columns={t_col: "unified_time", val_col: alias})
        win["SLAB_ID"], win["PROCEDURE_NAME"] = sid, procedure
        res.append(win[["SLAB_ID", "PROCEDURE_NAME", "unified_time", alias]])
    return pd.concat(res) if res else pd.DataFrame()


def align_and_merge_procedure_data(dfs, procedure, slab_id):
    if not dfs: return pd.DataFrame()
    m = pd.concat(dfs, ignore_index=True)
    m["unified_time"] = m["unified_time"].dt.floor("1s")
    aggs = {c: "mean" for c in m.columns if c not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]}
    return m.groupby(["SLAB_ID", "PROCEDURE_NAME", "unified_time"], as_index=False).agg(aggs)


# ====================== 4. 核心批处理流程 ======================
def process_batch(slab_ids, df_oper, cache, df_quality, discovered, stats_lookup):
    batch_list = []
    for sid in slab_ids:
        df_o_slab = df_oper[df_oper["SLAB_ID"] == sid]
        seqs = []
        for proc in df_o_slab["PROCEDURE_NAME"].unique():
            df_o_p = df_o_slab[df_o_slab["PROCEDURE_NAME"] == proc]
            dfs = []
            for fn in discovered.get(proc, []):
                if fn not in cache: continue
                cfg = get_file_config(fn)
                df_ts = extract_timesteps_by_time_window(cache[fn], df_o_p, cfg, proc, cfg["alias"])
                if not df_ts.empty: dfs.append(df_ts)
            if dfs: seqs.append(align_and_merge_procedure_data(dfs, proc, sid))
        if seqs: batch_list.append(pd.concat(seqs, ignore_index=True))

    if not batch_list: return pd.DataFrame(), 0, 0

    df_b = pd.concat(batch_list, ignore_index=True)
    df_b = df_b.merge(df_quality, on="SLAB_ID", how="left")

    # 预填充 0.0 为占位
    for col in ALL_ALIASES:
        if col not in df_b.columns:
            df_b[col] = 0.0
        else:
            df_b[col] = df_b[col].fillna(0.0)

    # 执行物理补全并获取统计
    df_b, cnt_a, cnt_b = impute_with_physics(df_b, stats_lookup)

    # 动态构建列顺序，确保静态质量列（other）被保留
    fixed = ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
    msk_cols = [c for c in df_b.columns if c.endswith("_MSK")]
    other_cols = [c for c in df_b.columns if c not in (fixed + ALL_ALIASES + msk_cols)]

    return df_b[fixed + ALL_ALIASES + msk_cols + other_cols], cnt_a, cnt_b


# ====================== 5. 主程序启动 ======================
def main():
    total_start_time = time.time()
    log_event("START", "==== 启动合表流水线 (物理分布敏感型) ====")

    df_quality, df_oper = load_base_tables()
    all_slab_ids = df_oper["SLAB_ID"].unique()
    total_slabs = len(all_slab_ids)
    log_event("INFO", f"总计处理 Slab_ID 数量: {total_slabs}")

    disco = discover_csv_files()
    stats_lookup = StatsFactory(RAW_TD_DIR, PROCESS_COLUMNS_CONFIG).analyze()

    # 预加载数据
    cache = {}
    all_f = set().union(*disco.values())
    log_event("STEP", f"正在预加载 {len(all_f)} 个过程数据文件到内存...")
    for f in all_f:
        try:
            cache[f] = load_process_data(f)
        except:
            continue

    out_path = os.path.join(OUTPUT_DIR, "process_timeseries_imputed_v3.csv")
    if os.path.exists(out_path): os.remove(out_path)

    first_write = True
    total_rows = 0
    num_batches = (total_slabs + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, total_slabs, BATCH_SIZE):
        batch = all_slab_ids[i:i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        log_event("PROCESS", f"批次 [{batch_idx}/{num_batches}] | 当前 Slab 规模: {len(batch)}")

        df_batch, ca, cb = process_batch(batch, df_oper, cache, df_quality, disco, stats_lookup)

        if not df_batch.empty:
            df_batch.to_csv(out_path, mode='a', header=first_write, index=False, encoding="utf-8-sig")
            first_write = False
            total_rows += len(df_batch)
            log_event("DETAIL", f"批次写入成功: 新增 {len(df_batch)} 行 | A类补全: {ca} 点 | B类填充: {cb} 点")

    log_event("SUCCESS", "==== 全流程任务圆满完成 ====")
    log_event("SUMMARY", f"最终保存路径: {out_path}")
    log_event("SUMMARY", f"总写入行数: {total_rows:,}")
    log_event("SUMMARY", f"总运行耗时: {time.time() - total_start_time:.2f} 秒")


if __name__ == "__main__":
    main()