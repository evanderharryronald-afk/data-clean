# import os
# import glob
# import time
# from typing import Dict, List, Optional
# import pandas as pd
# import numpy as np
# from config import OUTPUT_DIR, RAW_TD_DIR, RAW_OPER_TIME_FILE, RAW_QUALITY_FILE, PROCEDURE_FILES, \
#     PROCESS_COLUMNS_CONFIG, RAW_DATA_DIR
# from config import FILTERED_TD_DIR, FILTERED_OPER_TIME_FILE, FILTERED_QUALITY_FILE
#
# # Configuration Paths
# TD_DIR = RAW_DATA_DIR / "tb_min"
# OPER_TIME_FILE = RAW_OPER_TIME_FILE
# QUALITY_FILE = RAW_QUALITY_FILE
# OUT_PATH = OUTPUT_DIR / "process_timeseries_no_imputed_v5.csv"
#
#
# def log_event(level, msg):
#     timestamp = time.strftime('%H:%M:%S')
#     print(f"[{level} {timestamp}] {msg}")
#
#
# ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]
# TIME_PARSE_KWARGS = {"errors": "coerce"}
# BATCH_SIZE = 500
#
#
# # ====================== 1. StatsFactory: Physical DNA ======================
# class StatsFactory:
#     def __init__(self, raw_dir, config):
#         self.raw_dir = raw_dir
#         self.config = config
#
#     def analyze(self) -> Dict:
#         log_event("STEP", "Phase 0: Modeling physical DNA (Ambient values only)...")
#         stats_lookup = {}
#         for file_prefix, cfg in self.config.items():
#             alias = cfg['alias']
#             files = glob.glob(os.path.join(self.raw_dir, f"{file_prefix}*.csv"))
#             if not files: continue
#             try:
#                 df = pd.read_csv(files[0], usecols=['val'])
#                 combined = df['val'].dropna()
#             except:
#                 continue
#             if combined.empty: continue
#
#             force_zero_keywords = ['SPEED', 'FORCE', 'PRESS', 'RED', 'THICK']
#             is_mechanical = any(k in alias for k in force_zero_keywords)
#
#             if is_mechanical:
#                 ambient = 0.0
#             else:
#                 # Temperature: P05 as ambient floor
#                 ambient = max(combined.quantile(0.05), 20.0)
#
#             stats_lookup[alias] = {
#                 "ambient": ambient,
#                 "is_mechanical": is_mechanical
#             }
#         log_event("INFO", "Physical DNA modeling complete.")
#         return stats_lookup
#
#
# # ====================== 2. Physical Imputation (Simplified) ======================
# def impute_with_physics(df_batch: pd.DataFrame, stats_lookup: Dict):
#     """
#     仅保留B类填补（环境背景值），彻底移除A类（加工区填补）逻辑
#     """
#     mask_dict = {}
#     stats_b = 0
#
#     for col in ALL_ALIASES:
#         if col not in df_batch.columns: continue
#
#         # 识别实际观测值
#         is_real = (df_batch[col].abs() > 1e-2) & (df_batch[col].notnull())
#         mask_dict[f"{col}_MSK"] = is_real.astype('uint8')
#
#         stat = stats_lookup.get(col)
#         if not stat: continue
#
#         proc_prefix = col.split('_')[0]
#         in_work_zone = df_batch['PROCEDURE_NAME'].str.contains(proc_prefix, na=False)
#
#         # Type B: 结构化缺失 (仅在非加工区填补环境背景值)
#         mask_b = (~is_real) & (~in_work_zone)
#         if mask_b.any():
#             df_batch.loc[mask_b, col] = stat['ambient']
#             stats_b += mask_b.sum()
#
#     if mask_dict:
#         df_masks = pd.DataFrame(mask_dict, index=df_batch.index)
#         df_batch = pd.concat([df_batch, df_masks], axis=1)
#
#     return df_batch, 0, stats_b
#
#
# # ====================== 3. Utility Functions ======================
# def get_file_config(filename: str) -> Optional[Dict]:
#     base_name = os.path.splitext(filename)[0]
#     parts = base_name.split('_')
#     for i in range(len(parts), 0, -1):
#         prefix = '_'.join(parts[:i])
#         if prefix in PROCESS_COLUMNS_CONFIG: return PROCESS_COLUMNS_CONFIG[prefix]
#     return None
#
#
# def discover_csv_files():
#     discovered = {}
#     for proc, patterns in PROCEDURE_FILES.items():
#         actual = []
#         for p in patterns:
#             matching = glob.glob(os.path.join(TD_DIR, p))
#             actual.extend([os.path.basename(f) for f in matching if f.endswith('.csv')])
#         discovered[proc] = sorted(list(set(actual)))
#     return discovered
#
#
# def load_base_tables():
#     log_event("STEP", "Loading Excel configuration tables...")
#     df_q = pd.read_excel(QUALITY_FILE)
#     df_o = pd.read_excel(OPER_TIME_FILE)
#     if "SLAB_ID" not in df_q.columns:
#         df_q = df_q.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
#     df_o["START_TIME"] = pd.to_datetime(df_o["START_TIME"], **TIME_PARSE_KWARGS)
#     df_o["END_TIME"] = pd.to_datetime(df_o["END_TIME"], **TIME_PARSE_KWARGS)
#     return df_q, df_o
#
#
# def load_process_data(filename):
#     cfg = get_file_config(filename)
#     df = pd.read_csv(os.path.join(TD_DIR, filename))
#     t_col = cfg["time_col"]
#     df[t_col] = pd.to_datetime(df[t_col], **TIME_PARSE_KWARGS).dt.floor("1s")
#     return df.dropna(subset=[t_col]).sort_values(t_col)
#
#
# def extract_timesteps_by_time_window(df_proc, df_oper_proc, file_cfg, procedure, alias):
#     t_col = file_cfg["time_col"]
#     val_col = next(v for v in file_cfg["value_cols"] if v in df_proc.columns)
#     times, starts, ends, sids = df_proc[t_col].values, df_oper_proc["START_TIME"].values, df_oper_proc[
#         "END_TIME"].values, df_oper_proc["SLAB_ID"].values
#     res = []
#     for s, e, sid in zip(starts, ends, sids):
#         l, r = np.searchsorted(times, [s, e])
#         if l >= r: continue
#         win = df_proc.iloc[l:r].copy()
#         win = win.rename(columns={t_col: "unified_time", val_col: alias})
#         win["SLAB_ID"], win["PROCEDURE_NAME"] = sid, procedure
#         res.append(win[["SLAB_ID", "PROCEDURE_NAME", "unified_time", alias]])
#     return pd.concat(res) if res else pd.DataFrame()
#
#
# def align_and_merge_procedure_data(dfs, procedure, slab_id):
#     if not dfs: return pd.DataFrame()
#     m = pd.concat(dfs, ignore_index=True)
#     m["unified_time"] = m["unified_time"].dt.floor("1s")
#     aggs = {c: "mean" for c in m.columns if c not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]}
#     return m.groupby(["SLAB_ID", "PROCEDURE_NAME", "unified_time"], as_index=False).agg(aggs)
#
#
# # ====================== 4. Core Pipeline ======================
# def process_batch(slab_ids, df_oper, cache, df_quality, discovered, stats_lookup):
#     batch_list = []
#     for sid in slab_ids:
#         df_o_slab = df_oper[df_oper["SLAB_ID"] == sid]
#         seqs = []
#         for proc in df_o_slab["PROCEDURE_NAME"].unique():
#             df_o_p = df_o_slab[df_o_slab["PROCEDURE_NAME"] == proc]
#             dfs = []
#             for fn in discovered.get(proc, []):
#                 if fn not in cache: continue
#                 cfg = get_file_config(fn)
#                 df_ts = extract_timesteps_by_time_window(cache[fn], df_o_p, cfg, proc, cfg["alias"])
#                 if not df_ts.empty: dfs.append(df_ts)
#             if dfs: seqs.append(align_and_merge_procedure_data(dfs, proc, sid))
#         if seqs: batch_list.append(pd.concat(seqs, ignore_index=True))
#
#     if not batch_list: return pd.DataFrame(), 0, 0
#
#     df_b = pd.concat(batch_list, ignore_index=True)
#
#     # 修改点 1: 使用 how='inner' 强制丢弃没有质量数据的 SLAB_ID
#     df_b = df_b.merge(df_quality, on="SLAB_ID", how="inner")
#
#     if df_b.empty: return pd.DataFrame(), 0, 0
#
#     for col in ALL_ALIASES:
#         if col not in df_b.columns:
#             df_b[col] = 0.0
#         else:
#             # 不再进行物理填补，仅将真正的空值转为 0
#             df_b[col] = df_b[col].fillna(0.0)
#
#     # 修改点 2: 内部已移除 A 类填补逻辑
#     df_b, cnt_a, cnt_b = impute_with_physics(df_b, stats_lookup)
#
#     fixed = ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
#     msk_cols = [c for c in df_b.columns if c.endswith("_MSK")]
#     other_cols = [c for c in df_b.columns if c not in (fixed + ALL_ALIASES + msk_cols)]
#     return df_b[fixed + ALL_ALIASES + msk_cols + other_cols], cnt_a, cnt_b
#
#
# def main():
#     total_start_time = time.time()
#     log_event("START", "==== Launching Clean Data Pipeline v5 (Strict Filtering) ====")
#
#     df_quality, df_oper = load_base_tables()
#     # 预过滤：仅保留在质量表中存在的 SLAB_ID
#     valid_slab_ids_in_q = set(df_quality["SLAB_ID"].unique())
#     all_slab_ids = [sid for sid in df_oper["SLAB_ID"].unique() if sid in valid_slab_ids_in_q]
#
#     total_slabs = len(all_slab_ids)
#     log_event("INFO", f"Filtered Total Slab_ID to process (with Quality data): {total_slabs}")
#
#     disco = discover_csv_files()
#     stats_lookup = StatsFactory(TD_DIR, PROCESS_COLUMNS_CONFIG).analyze()
#
#     cache = {}
#     all_f = set().union(*disco.values())
#     log_event("STEP", f"Pre-loading {len(all_f)} process files...")
#     for f in all_f:
#         try:
#             cache[f] = load_process_data(f)
#         except:
#             continue
#
#     if os.path.exists(OUT_PATH): os.remove(OUT_PATH)
#
#     first_write = True
#     total_rows = 0
#     num_batches = (total_slabs + BATCH_SIZE - 1) // BATCH_SIZE
#
#     for i in range(0, total_slabs, BATCH_SIZE):
#         batch = all_slab_ids[i:i + BATCH_SIZE]
#         batch_idx = i // BATCH_SIZE + 1
#         log_event("PROCESS", f"Batch [{batch_idx}/{num_batches}] | Size: {len(batch)}")
#
#         df_batch, ca, cb = process_batch(batch, df_oper, cache, df_quality, disco, stats_lookup)
#
#         if not df_batch.empty:
#             df_batch.to_csv(OUT_PATH, mode='a', header=first_write, index=False, encoding="utf-8-sig")
#             first_write = False
#             total_rows += len(df_batch)
#             log_event("DETAIL", f"Batch saved: +{len(df_batch)} rows | Ambient Fill: {cb} pts")
#
#     log_event("SUCCESS", "==== Pipeline Complete ====")
#     log_event("SUMMARY", f"Final Output: {OUT_PATH}")
#     log_event("SUMMARY", f"Total Rows: {total_rows:,}")
#     log_event("SUMMARY", f"Execution Time: {time.time() - total_start_time:.2f}s")
#
#
# if __name__ == "__main__":
#     main()
import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from config import OUTPUT_DIR, RAW_TD_DIR, RAW_OPER_TIME_FILE, RAW_QUALITY_FILE, PROCEDURE_FILES, \
    PROCESS_COLUMNS_CONFIG, RAW_DATA_DIR

# Configuration Paths
TD_DIR = RAW_DATA_DIR / "tb_min"
OPER_TIME_FILE = RAW_OPER_TIME_FILE
QUALITY_FILE = RAW_QUALITY_FILE
OUT_PATH = OUTPUT_DIR / "process_timeseries_no_imputed_v5.csv"
MISSING_REPORT_PATH = OUTPUT_DIR / "missing_quality_slabs_report.csv"


def log_event(level, msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{level} {timestamp}] {msg}")


ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]
TIME_PARSE_KWARGS = {"errors": "coerce"}
BATCH_SIZE = 500


# ====================== 1. StatsFactory ======================
class StatsFactory:
    def __init__(self, raw_dir, config):
        self.raw_dir = raw_dir
        self.config = config

    def analyze(self) -> Dict:
        log_event("STEP", "Phase 0: Modeling physical DNA (Ambient values only)...")
        stats_lookup = {}
        for file_prefix, cfg in self.config.items():
            alias = cfg['alias']
            files = glob.glob(os.path.join(self.raw_dir, f"{file_prefix}*.csv"))
            if not files: continue
            try:
                df = pd.read_csv(files[0], usecols=['val'])
                combined = df['val'].dropna()
            except:
                continue
            if combined.empty: continue

            force_zero_keywords = ['SPEED', 'FORCE', 'PRESS', 'RED', 'THICK']
            is_mechanical = any(k in alias for k in force_zero_keywords)
            ambient = 0.0 if is_mechanical else max(combined.quantile(0.05), 20.0)

            stats_lookup[alias] = {"ambient": ambient, "is_mechanical": is_mechanical}
        log_event("INFO", "Physical DNA modeling complete.")
        return stats_lookup


# ====================== 2. Physical Imputation (Simplified) ======================
def impute_with_physics(df_batch: pd.DataFrame, stats_lookup: Dict):
    mask_dict = {}
    stats_b = 0
    for col in ALL_ALIASES:
        if col not in df_batch.columns: continue
        is_real = (df_batch[col].abs() > 1e-2) & (df_batch[col].notnull())
        mask_dict[f"{col}_MSK"] = is_real.astype('uint8')

        stat = stats_lookup.get(col)
        if not stat: continue

        proc_prefix = col.split('_')[0]
        in_work_zone = df_batch['PROCEDURE_NAME'].str.contains(proc_prefix, na=False)
        mask_b = (~is_real) & (~in_work_zone)
        if mask_b.any():
            df_batch.loc[mask_b, col] = stat['ambient']
            stats_b += mask_b.sum()

    if mask_dict:
        df_masks = pd.DataFrame(mask_dict, index=df_batch.index)
        df_batch = pd.concat([df_batch, df_masks], axis=1)
    return df_batch, 0, stats_b


# ====================== 3. Utility Functions ======================
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
            matching = glob.glob(os.path.join(TD_DIR, p))
            actual.extend([os.path.basename(f) for f in matching if f.endswith('.csv')])
        discovered[proc] = sorted(list(set(actual)))
    return discovered


def load_base_tables():
    log_event("STEP", "Loading Excel configuration tables...")
    df_q = pd.read_excel(QUALITY_FILE)
    df_o = pd.read_excel(OPER_TIME_FILE)
    if "SLAB_ID" not in df_q.columns:
        df_q = df_q.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
    df_o["START_TIME"] = pd.to_datetime(df_o["START_TIME"], **TIME_PARSE_KWARGS)
    df_o["END_TIME"] = pd.to_datetime(df_o["END_TIME"], **TIME_PARSE_KWARGS)
    return df_q, df_o


def load_process_data(filename):
    cfg = get_file_config(filename)
    df = pd.read_csv(os.path.join(TD_DIR, filename))
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


# ====================== 4. Core Pipeline ======================
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
    # 核心修改：Inner Join 过滤
    df_b = df_b.merge(df_quality, on="SLAB_ID", how="inner")
    if df_b.empty: return pd.DataFrame(), 0, 0

    for col in ALL_ALIASES:
        df_b[col] = df_b[col].fillna(0.0) if col in df_b.columns else 0.0

    df_b, cnt_a, cnt_b = impute_with_physics(df_b, stats_lookup)
    fixed = ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
    msk_cols = [c for c in df_b.columns if c.endswith("_MSK")]
    other_cols = [c for c in df_b.columns if c not in (fixed + ALL_ALIASES + msk_cols)]
    return df_b[fixed + ALL_ALIASES + msk_cols + other_cols], cnt_a, cnt_b


def main():
    total_start_time = time.time()
    log_event("START", "==== Launching Clean Data Pipeline v5.1 (Filtering & Stats) ====")

    df_quality, df_oper = load_base_tables()

    # --- 新增：缺失质量数据统计逻辑 ---
    oper_slabs = set(df_oper["SLAB_ID"].unique())
    quality_slabs = set(df_quality["SLAB_ID"].unique())
    missing_quality_slabs = list(oper_slabs - quality_slabs)

    if missing_quality_slabs:
        log_event("WARNING", f"Found {len(missing_quality_slabs)} slabs without quality data.")
        missing_df = pd.DataFrame(missing_quality_slabs, columns=["MISSING_SLAB_ID"])
        # 记录缺失详情：哪些工序受影响
        missing_detail = df_oper[df_oper["SLAB_ID"].isin(missing_quality_slabs)].groupby("SLAB_ID")[
            "PROCEDURE_NAME"].unique().reset_index()
        missing_detail.to_csv(MISSING_REPORT_PATH, index=False, encoding="utf-8-sig")
        log_event("INFO", f"Detailed missing report saved to: {MISSING_REPORT_PATH}")
    else:
        log_event("INFO", "Data Integrity Check: All slabs have quality labels.")

    # 预过滤：仅处理有质量数据的 ID
    all_slab_ids = [sid for sid in df_oper["SLAB_ID"].unique() if sid in quality_slabs]
    total_slabs = len(all_slab_ids)
    log_event("INFO", f"Active processing queue: {total_slabs} slabs.")

    disco = discover_csv_files()
    stats_lookup = StatsFactory(TD_DIR, PROCESS_COLUMNS_CONFIG).analyze()

    cache = {}
    all_f = set().union(*disco.values())
    log_event("STEP", f"Pre-loading {len(all_f)} process files...")
    for f in all_f:
        try:
            cache[f] = load_process_data(f)
        except:
            continue

    if os.path.exists(OUT_PATH): os.remove(OUT_PATH)

    first_write = True
    total_rows = 0
    num_batches = (total_slabs + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, total_slabs, BATCH_SIZE):
        batch = all_slab_ids[i:i + BATCH_SIZE]
        log_event("PROCESS", f"Batch [{i // BATCH_SIZE + 1}/{num_batches}] | Size: {len(batch)}")
        df_batch, ca, cb = process_batch(batch, df_oper, cache, df_quality, disco, stats_lookup)

        if not df_batch.empty:
            df_batch.to_csv(OUT_PATH, mode='a', header=first_write, index=False, encoding="utf-8-sig")
            first_write = False
            total_rows += len(df_batch)
            log_event("DETAIL", f"Progress: +{len(df_batch)} rows saved.")

    log_event("SUCCESS", "==== Pipeline Complete ====")
    log_event("SUMMARY", f"Final Output: {OUT_PATH}")
    log_event("SUMMARY", f"Total Records Processed: {total_rows:,}")
    log_event("SUMMARY", f"Total Time: {time.time() - total_start_time:.2f}s")


if __name__ == "__main__":
    main()