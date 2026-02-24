import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

"""
时序数据合表 - 最终简化版（符合用户最新要求）
- quality 列完全忽略
- 缺失值统一填 0
- 排序只按 SLAB_ID + unified_time（全局时间序）
- ts 无重复 → 去掉同一秒聚合，直接 concat + sort
- 缺失工序/文件：不生成占位行，只保留有真实时间步的记录
"""

BASE_DIR = 'E:/SGAI_Project/data_clean'
TD_DIR = os.path.join(BASE_DIR, "tb")  # TD数据目录
OUTPUT_DIR = os.path.join(BASE_DIR, "output")  # 输出目录

QUALITY_FILE = os.path.join(BASE_DIR, "select_v_quality_all.xlsx")
OPER_TIME_FILE = os.path.join(BASE_DIR, "v_jk_oper_time.xlsx")

PROCEDURE_FILES: Dict[str, List[str]] = {
    "RM": [
        "tb_rm_speed_meas_*.csv",
        "tb_rm_roll_force_*.csv",
        "tb_rm_red_meas_*.csv",
        "tb_rm_pyro_meter_load_before_*.csv",
        "tb_rm_pyro_meter_load_after_*.csv",
    ],
    "FM": [
        "tb_fm_speed_meas_*.csv",
        "tb_fm_roll_force_meas_*.csv",
        "tb_fm_red_meas_*.csv",
        "tb_fm_pyro_meter_load_before_*.csv",
        "tb_fm_pyro_meter_load_after_*.csv",
        "tb_fm_exit_os_thick_meas_*.csv",
        "tb_fm_exit_ds_thick_meas_*.csv",
    ],
    "PPL": [
        "tb_pre_leveller_speed_*.csv",
        "tb_pre_leveller_force_all_*.csv",
        "tb_pre_leveller_pyro_meter_load_after_*.csv",
    ],
    "UFC": [
        "tb_ufc_temp_meas_*.csv",
        "tb_ufc_water_press_set_*.csv",
        "tb_ufc_spiny_water_ratio_*.csv",
    ],
    "ACC": [
        "tb_acc_pyro_meter_load_before_*.csv",
        "tb_acc_pyro_meter_load_after_*.csv",
        "tb_acc_water_press_set_*.csv",
        "tb_acc_spiny_water_ratio_*.csv",
    ],
    "HPL": [
        "tb_hot_leveller_temp_entry_*.csv",
    ],
    "DESCALING": [
        "tb_desc_water_press_entry_*.csv",
        "tb_desc_water_press_exit_*.csv",
        "tb_desc_pyro_meter_load_after_*.csv",
    ],
}

PROCESS_COLUMNS_CONFIG: Dict[str, Dict] = {
    "tb_rm_speed_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "RM_SPEED"},
    "tb_rm_roll_force": {"time_col": "ts", "value_cols": ["val"], "alias": "RM_FORCE"},
    "tb_rm_red_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "RM_RED"},
    "tb_rm_pyro_meter_load_before": {"time_col": "ts", "value_cols": ["val"], "alias": "RM_PYRO_BEFORE"},
    "tb_rm_pyro_meter_load_after": {"time_col": "ts", "value_cols": ["val"], "alias": "RM_PYRO_AFTER"},
    "tb_fm_speed_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_SPEED"},
    "tb_fm_roll_force_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_FORCE"},
    "tb_fm_red_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_RED"},
    "tb_fm_pyro_meter_load_before": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_PYRO_BEFORE"},
    "tb_fm_pyro_meter_load_after": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_PYRO_AFTER"},
    "tb_fm_exit_os_thick_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_EXIT_OS_THICK"},
    "tb_fm_exit_ds_thick_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "FM_EXIT_DS_THICK"},
    "tb_pre_leveller_speed": {"time_col": "ts", "value_cols": ["val"], "alias": "PPL_SPEED"},
    "tb_pre_leveller_force_all": {"time_col": "ts", "value_cols": ["val"], "alias": "PPL_FORCE"},
    "tb_pre_leveller_pyro_meter_load_after": {"time_col": "ts", "value_cols": ["val"], "alias": "PPL_PYRO_AFTER"},
    "tb_ufc_temp_meas": {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_TEMP"},
    "tb_ufc_water_press_set": {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_WATER_PRESS"},
    "tb_ufc_spiny_water_ratio": {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_SPINY_WATER_RATIO"},
    "tb_acc_pyro_meter_load_before": {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_PYRO_BEFORE"},
    "tb_acc_pyro_meter_load_after": {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_PYRO_AFTER"},
    "tb_acc_water_press_set": {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_WATER_PRESS"},
    "tb_acc_spiny_water_ratio": {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_SPINY_WATER_RATIO"},
    "tb_hot_leveller_temp_entry": {"time_col": "ts", "value_cols": ["val"], "alias": "HPL_TEMP_ENTRY"},
    "tb_desc_water_press_entry": {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_WATER_PRESS_ENTRY"},
    "tb_desc_water_press_exit": {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_WATER_PRESS_EXIT"},
    "tb_desc_pyro_meter_load_after": {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_PYRO_AFTER"},
}

TIME_PARSE_KWARGS = {"errors": "coerce"}
BATCH_SIZE = 500
# BATCH_SIZE=20

def get_file_config(filename: str) -> Optional[Dict]:
    base_name = os.path.splitext(filename)[0]
    parts = base_name.split('_')
    for i in range(len(parts), 0, -1):
        prefix = '_'.join(parts[:i])
        if prefix in PROCESS_COLUMNS_CONFIG:
            return PROCESS_COLUMNS_CONFIG[prefix]
    return None

def discover_csv_files() -> Dict[str, List[str]]:
    print("自动发现CSV文件 ...")
    discovered_files: Dict[str, List[str]] = {}
    for procedure, patterns in PROCEDURE_FILES.items():
        actual_files = []
        for pattern in patterns:
            search_path = os.path.join(TD_DIR, pattern)
            matching = glob.glob(search_path)
            csv_files = [os.path.basename(f) for f in matching if f.endswith('.csv')]
            actual_files.extend(csv_files)
        actual_files = sorted(list(set(actual_files)))
        discovered_files[procedure] = actual_files
        print(f"  {procedure}: 发现 {len(actual_files)} 个文件")
    return discovered_files

def load_base_tables():
    print("读取质量表和工序时间表 ...")
    df_quality = pd.read_excel(QUALITY_FILE)
    df_oper = pd.read_excel(OPER_TIME_FILE)

    if "SLAB_ID" not in df_quality.columns:
        if "FUR_EXIT_SLAB_ID" in df_quality.columns:
            df_quality = df_quality.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
        else:
            raise ValueError("质量表缺少 SLAB_ID 或 FUR_EXIT_SLAB_ID")

    required = ["SLAB_ID", "PROCEDURE_NAME", "START_TIME", "END_TIME"]
    for col in required:
        if col not in df_oper.columns:
            raise ValueError(f"工序时间表缺少列: {col}")

    df_oper["START_TIME"] = pd.to_datetime(df_oper["START_TIME"], **TIME_PARSE_KWARGS)
    df_oper["END_TIME"] = pd.to_datetime(df_oper["END_TIME"], **TIME_PARSE_KWARGS)

    target_procs = list(PROCEDURE_FILES.keys())
    df_oper = df_oper[df_oper["PROCEDURE_NAME"].isin(target_procs)].copy()
    df_oper = df_oper.sort_values(["SLAB_ID", "START_TIME"])

    return df_quality, df_oper

def load_process_data(filename: str) -> pd.DataFrame:
    cfg = get_file_config(filename)
    if cfg is None:
        raise ValueError(f"无配置: {filename}")

    path = os.path.join(TD_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    print(f"读取: {filename}")
    df = pd.read_csv(path)

    time_col = cfg["time_col"]
    if time_col not in df.columns:
        raise ValueError(f"{filename} 无时间列 {time_col}")

    df[time_col] = pd.to_datetime(df[time_col], **TIME_PARSE_KWARGS)
    if df[time_col].dtype == "datetime64[ns]":
        df[time_col] = df[time_col].dt.floor("1s")

    df = df.dropna(subset=[time_col])
    # ===== 关键优化：按时间排序（只加这一行）=====
    df = df.sort_values(time_col).reset_index(drop=True)
    return df

# def extract_timesteps_by_time_window(
#     df_proc: pd.DataFrame,
#     df_oper_proc: pd.DataFrame,
#     file_cfg: Dict,
#     procedure: str,
#     filename: str,
# ) -> pd.DataFrame:
#     time_col = file_cfg["time_col"]
#     value_cols = file_cfg["value_cols"]
#     alias = file_cfg.get("alias", os.path.splitext(filename)[0]).strip()
#
#     all_rows = []
#
#     for _, op in df_oper_proc.iterrows():
#         slab_id = op["SLAB_ID"]
#         start = op["START_TIME"]
#         end = op["END_TIME"]
#
#         df_tmp = df_proc
#         mask = (df_tmp[time_col] >= start) & (df_tmp[time_col] <= end)
#
#         actual_col = next((vc for vc in value_cols if vc in df_tmp.columns), None)
#         if actual_col is None:
#             raise ValueError(f"{filename} 无值列 {value_cols}")
#
#         df_window = df_tmp.loc[mask, [time_col, actual_col]].copy()
#
#         if df_window.empty:
#             continue  # 不生成占位行
#
#         df_window["SLAB_ID"] = slab_id
#         df_window["PROCEDURE_NAME"] = procedure
#         df_window = df_window.rename(columns={time_col: "unified_time", actual_col: alias})
#         all_rows.append(df_window)
#
#     if not all_rows:
#         return pd.DataFrame()
#
#     return pd.concat(all_rows, ignore_index=True)

# def extract_timesteps_by_time_window(
#     df_proc: pd.DataFrame,
#     df_oper_proc: pd.DataFrame,
#     file_cfg: Dict,
#     procedure: str,
#     filename: str,
# ) -> pd.DataFrame:
#     time_col = file_cfg["time_col"]
#     value_cols = file_cfg["value_cols"]
#     alias = file_cfg.get("alias", os.path.splitext(filename)[0]).strip()
#
#     all_rows = []
#
#     # numpy array，极快
#     times = df_proc[time_col].values
#     actual_col = next((vc for vc in value_cols if vc in df_proc.columns), None)
#     if actual_col is None:
#         raise ValueError(f"{filename} 无值列 {value_cols}")
#
#     # 提前拿到列索引，避免每次查找
#     time_idx = df_proc.columns.get_loc(time_col)
#     value_idx = df_proc.columns.get_loc(actual_col)
#
#     for _, op in df_oper_proc.iterrows():
#         slab_id = op["SLAB_ID"]
#         start = op["START_TIME"]
#         end = op["END_TIME"]
#
#         # 二分查找区间 —— 这就是提速的核心！
#         left = np.searchsorted(times, start, side='left')
#         right = np.searchsorted(times, end, side='right')
#
#         if left >= right:
#             continue
#
#         # 直接切片（只复制需要的几百行）
#         df_window = df_proc.iloc[left:right, [time_idx, value_idx]].copy()
#
#         df_window["SLAB_ID"] = slab_id
#         df_window["PROCEDURE_NAME"] = procedure
#         df_window = df_window.rename(columns={time_col: "unified_time", actual_col: alias})
#         all_rows.append(df_window)
#
#     if not all_rows:
#         return pd.DataFrame()
#
#     return pd.concat(all_rows, ignore_index=True)



def extract_timesteps_by_time_window(
    df_proc: pd.DataFrame,
    df_oper_proc: pd.DataFrame,
    file_cfg: Dict,
    procedure: str,
    filename: str,
) -> pd.DataFrame:
    if df_oper_proc.empty:
        return pd.DataFrame()

    time_col = file_cfg["time_col"]
    actual_col = next((vc for vc in file_cfg["value_cols"] if vc in df_proc.columns), None)
    if actual_col is None:
        return pd.DataFrame()

    alias = file_cfg.get("alias", os.path.splitext(filename)[0]).strip()

    times = df_proc[time_col].values
    starts = df_oper_proc["START_TIME"].values
    ends = df_oper_proc["END_TIME"].values
    slab_ids = df_oper_proc["SLAB_ID"].values

    all_dfs = []
    for start, end, slab_id in zip(starts, ends, slab_ids):
        left = np.searchsorted(times, start, side='left')
        right = np.searchsorted(times, end, side='right')
        if left >= right:
            continue
        df_win = df_proc.iloc[left:right, [df_proc.columns.get_loc(time_col), df_proc.columns.get_loc(actual_col)]].copy()
        df_win["SLAB_ID"] = slab_id
        df_win["PROCEDURE_NAME"] = procedure
        df_win = df_win.rename(columns={time_col: "unified_time", actual_col: alias})
        all_dfs.append(df_win)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)





def align_and_merge_procedure_data(
    dfs_by_file: List[pd.DataFrame], procedure: str, slab_id: str
) -> pd.DataFrame:
    if not dfs_by_file:
        return pd.DataFrame()

    all_process_cols = set()
    for df in dfs_by_file:
        for col in df.columns:
            if col not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]:
                all_process_cols.add(col)

    df_merged = pd.concat(dfs_by_file, ignore_index=True)
    df_merged = df_merged[df_merged["unified_time"].notna()].copy()

    if df_merged.empty:
        return pd.DataFrame()

    df_merged = df_merged.sort_values("unified_time")

    # 补齐缺失特征列为 0
    for col in all_process_cols:
        if col not in df_merged.columns:
            df_merged[col] = 0
    df_merged = df_merged.groupby(['SLAB_ID', 'PROCEDURE_NAME', 'unified_time']).max().reset_index()

    # ===== 新增：固定特征列顺序（RM→FM→PPL→...）=====
    ordered_cols = ['SLAB_ID', 'PROCEDURE_NAME', 'unified_time']
    process_order = [
        cfg.get("alias", "").strip()
        for cfg in PROCESS_COLUMNS_CONFIG.values()
        if cfg.get("alias", "").strip()
    ]
    ordered_cols += [c for c in process_order if c in df_merged.columns]
    other_cols = [c for c in df_merged.columns if c not in ordered_cols]
    df_merged = df_merged[ordered_cols + other_cols]

    return df_merged

def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality, discovered_files):
    all_sequences = []

    for slab_id in slab_ids_batch:
        start_time = time.time()
        df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id]
        if len(df_oper_slab) > 24 or len(df_oper_slab) < 16:  # 阈值可调
            print(f"  跳过异常 SLAB_ID: {slab_id} （工序数 {len(df_oper_slab)}  异常）")
            continue

        print(f"  开始处理 SLAB_ID: {slab_id}   (工序数: {len(df_oper_slab)})")

        for proc in df_oper_slab["PROCEDURE_NAME"].unique():
            df_oper_proc = df_oper_slab[df_oper_slab["PROCEDURE_NAME"] == proc]

            dfs_by_file = []

            for filename in discovered_files.get(proc, []):
                file_cfg = get_file_config(filename)
                if not file_cfg or filename not in process_data_cache:
                    continue

                df_ts = extract_timesteps_by_time_window(
                    process_data_cache[filename],
                    df_oper_proc,
                    file_cfg,
                    proc,
                    filename
                )

                if not df_ts.empty:
                    dfs_by_file.append(df_ts)

            if dfs_by_file:
                df_aligned = align_and_merge_procedure_data(dfs_by_file, proc, slab_id)
                if not df_aligned.empty:
                    all_sequences.append(df_aligned)
            print(f"    完成 SLAB_ID: {slab_id}，耗时 {time.time() - start_time:.1f} 秒")
    if not all_sequences:
        return pd.DataFrame()

    df_batch_ts = pd.concat(all_sequences, ignore_index=True)

    # 关键修改：只按 SLAB_ID + unified_time 排序
    df_batch_ts = df_batch_ts.sort_values(["SLAB_ID", "unified_time"])

    df_batch_final = df_batch_ts.merge(df_quality, on="SLAB_ID", how="left")

    # 目标变量过滤（全NA或全0删除）
    target_cols = ["TS", "YIELD_REH", "YIELD_REL", "HOMO_EL", "YIELD_RATE", "IMPACT_AVG"]
    existing_targets = [c for c in target_cols if c in df_batch_final.columns]

    if existing_targets:
        mask_na = df_batch_final[existing_targets].isna().all(axis=1)
        mask_zero = (df_batch_final[existing_targets].fillna(0) == 0).all(axis=1)
        df_batch_final = df_batch_final[~(mask_na | mask_zero)].copy()

    # 缺失值填 0（关键修改）
    process_cols = [cfg.get("alias", "").strip() for cfg in PROCESS_COLUMNS_CONFIG.values() if cfg.get("alias", "").strip()]
    process_cols = list(set(c for c in process_cols if c in df_batch_final.columns))
    if process_cols:
        df_batch_final[process_cols] = df_batch_final[process_cols].fillna(0)

    # 时间格式化（保持 datetime 或转字符串，根据下游需要）
    # 如果 LSTM 需要 datetime，建议不转字符串；这里按原代码保留字符串格式
    df_batch_final["unified_time"] = df_batch_final["unified_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df_batch_final

# def main():
#     df_quality, df_oper = load_base_tables()
#     print("每个 SLAB_ID 的工序数量分布：")
#     print(df_oper.groupby("SLAB_ID").size().value_counts().sort_index())
#     print("\n工序数量最多的前5个 SLAB_ID：")
#     print(df_oper.groupby("SLAB_ID").size().nlargest(5))
#
#     discovered_files = discover_csv_files()
#
#     process_data_cache = {}
#     all_files = set().union(*discovered_files.values())
#
#     print("预加载过程数据表...")
#     for fn in all_files:
#         try:
#             process_data_cache[fn] = load_process_data(fn)
#         except Exception as e:
#             print(f"读取 {fn} 失败：{e}，跳过")
#
#     unique_slab_ids = df_oper["SLAB_ID"].unique()
#     print(f"总 SLAB_ID 数量: {len(unique_slab_ids)}")
#
#     out_path = os.path.join(OUTPUT_DIR, "process_timeseries_clean.csv")
#
#     if os.path.exists(out_path):
#         os.remove(out_path)
#
#     total_processed = 0
#
#     for i in range(0, len(unique_slab_ids), BATCH_SIZE):
#         batch = unique_slab_ids[i:i + BATCH_SIZE]
#         print(f"处理批次 {i//BATCH_SIZE + 1}，{len(batch)} 个 SLAB_ID")
#
#         result = process_batch(batch, df_oper, process_data_cache, df_quality, discovered_files)
#
#         if not result.empty:
#             header = (total_processed == 0)
#             result.to_csv(out_path, mode='a', header=header, index=False, encoding="utf-8-sig")
#             total_processed += len(result)
#             print(f"  写入 {len(result)} 行，累计 {total_processed}")
#
#         del result
#
#     if os.path.exists(out_path) and total_processed > 0:
#         print("正在进行最终全局排序（保证严格按 SLAB_ID + unified_time）...")
#         df_all = pd.read_csv(out_path)
#         df_all = df_all.sort_values(["SLAB_ID", "unified_time"])
#         df_all.to_csv(out_path, index=False, encoding="utf-8-sig")
#         print("全局排序完成")
#
#     print(f"\n完成。输出文件: {out_path}")
#     print(f"总行数: {total_processed}")
#
#     if total_processed > 0:
#         temp = pd.read_csv(out_path, nrows=0)
#         print(f"列数: {temp.shape[1]}")
#
#         sample = pd.read_csv(out_path, nrows=10000)
#         print(f"唯一SLAB_ID: {sample['SLAB_ID'].nunique()}")
#         print(f"唯一工序: {sorted(sample['PROCEDURE_NAME'].unique())}")


def main():
    df_quality, df_oper = load_base_tables()
    print("每个 SLAB_ID 的工序数量分布：")
    print(df_oper.groupby("SLAB_ID").size().value_counts().sort_index())
    print("\n工序数量最多的前5个 SLAB_ID：")
    print(df_oper.groupby("SLAB_ID").size().nlargest(5))

    discovered_files = discover_csv_files()

    process_data_cache = {}
    all_files = set().union(*discovered_files.values())

    print("预加载过程数据表...")
    for fn in all_files:
        try:
            process_data_cache[fn] = load_process_data(fn)
        except Exception as e:
            print(f"读取 {fn} 失败：{e}，跳过")

    unique_slab_ids = df_oper["SLAB_ID"].unique()
    print(f"总 SLAB_ID 数量: {len(unique_slab_ids)}")

    out_path = os.path.join(OUTPUT_DIR, "process_timeseries_clean.csv")

    # 如果已存在，先删除（避免旧文件干扰）
    if os.path.exists(out_path):
        os.remove(out_path)

    all_results = []           # 收集所有 batch 的结果
    total_processed = 0

    for i in range(0, len(unique_slab_ids), BATCH_SIZE):
        batch = unique_slab_ids[i:i + BATCH_SIZE]
        print(f"处理批次 {i//BATCH_SIZE + 1}，{len(batch)} 个 SLAB_ID")

        result = process_batch(batch, df_oper, process_data_cache, df_quality, discovered_files)

        if not result.empty:
            # 时间范围统计（每个 batch 单独打印和保存）
            time_stats = result.groupby("SLAB_ID")["unified_time"].agg(
                ['min', 'max', lambda x: (pd.to_datetime(x.max()) - pd.to_datetime(x.min())).total_seconds() / 60]
            )
            time_stats.columns = ['time_min', 'time_max', 'duration_min']
            print("SLAB_ID 时间范围统计：\n", time_stats)
            time_stats.to_csv(
                os.path.join(OUTPUT_DIR, f"slab_time_stats_batch_{i//BATCH_SIZE + 1}.csv"),
                encoding="utf-8-sig"
            )

            all_results.append(result)
            total_processed += len(result)
            print(f"  暂存 {len(result)} 行，累计 {total_processed}")
        else:
            print("  本批次无有效数据")

        del result  # 及时释放内存

    # 所有批次处理完毕后，合并、排序、写入
    if all_results:
        print("\n合并所有批次结果并进行最终全局排序...")
        df_final = pd.concat(all_results, ignore_index=True)
        df_final = df_final.sort_values(["SLAB_ID", "unified_time"])

        # 一次性写入最终 CSV
        df_final.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"完成。输出文件: {out_path}")
        print(f"总行数: {len(df_final)}")

        # 后续统计（列数、样本信息）
        if os.path.exists(out_path):
            temp = pd.read_csv(out_path, nrows=0)
            print(f"列数: {temp.shape[1]}")

            sample = pd.read_csv(out_path, nrows=10000)
            print(f"唯一SLAB_ID: {sample['SLAB_ID'].nunique()}")
            print(f"唯一工序: {sorted(sample['PROCEDURE_NAME'].unique())}")
    else:
        print("\n所有批次均无有效数据，未生成输出文件")

    print("\n处理结束。")



if __name__ == "__main__":
    main()