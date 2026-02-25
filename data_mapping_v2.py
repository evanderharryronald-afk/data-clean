import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

"""
时序数据合表 - 修复版
修复内容：
1. 同一秒两行问题：最终合并时按 ['SLAB_ID', 'unified_time'] 做全局去重（去掉 PROCEDURE_NAME 作为 groupby key）
2. 空值列问题：在 align_and_merge 内部提前 fillna(0)，并在 process_batch 末尾补全所有 alias 列
3. 时间窗口边界重叠：end 侧改为 side='left'，避免相邻工序共享边界秒
4. 工序数阈值：不再跳过异常 SLAB_ID，改为警告后继续处理
5. print 位置修复：完成 SLAB_ID 的日志移到工序循环外
6. 内存管理：all_results 收集时不再无效 del result
"""

BASE_DIR = 'E:/SGAI_Project/data_clean'
TD_DIR = os.path.join(BASE_DIR, "tb")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

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
    "tb_rm_speed_meas":                     {"time_col": "ts", "value_cols": ["val"], "alias": "RM_SPEED"},
    "tb_rm_roll_force":                     {"time_col": "ts", "value_cols": ["val"], "alias": "RM_FORCE"},
    "tb_rm_red_meas":                       {"time_col": "ts", "value_cols": ["val"], "alias": "RM_RED"},
    "tb_rm_pyro_meter_load_before":         {"time_col": "ts", "value_cols": ["val"], "alias": "RM_PYRO_BEFORE"},
    "tb_rm_pyro_meter_load_after":          {"time_col": "ts", "value_cols": ["val"], "alias": "RM_PYRO_AFTER"},
    "tb_fm_speed_meas":                     {"time_col": "ts", "value_cols": ["val"], "alias": "FM_SPEED"},
    "tb_fm_roll_force_meas":                {"time_col": "ts", "value_cols": ["val"], "alias": "FM_FORCE"},
    "tb_fm_red_meas":                       {"time_col": "ts", "value_cols": ["val"], "alias": "FM_RED"},
    "tb_fm_pyro_meter_load_before":         {"time_col": "ts", "value_cols": ["val"], "alias": "FM_PYRO_BEFORE"},
    "tb_fm_pyro_meter_load_after":          {"time_col": "ts", "value_cols": ["val"], "alias": "FM_PYRO_AFTER"},
    "tb_fm_exit_os_thick_meas":             {"time_col": "ts", "value_cols": ["val"], "alias": "FM_EXIT_OS_THICK"},
    "tb_fm_exit_ds_thick_meas":             {"time_col": "ts", "value_cols": ["val"], "alias": "FM_EXIT_DS_THICK"},
    "tb_pre_leveller_speed":                {"time_col": "ts", "value_cols": ["val"], "alias": "PPL_SPEED"},
    "tb_pre_leveller_force_all":            {"time_col": "ts", "value_cols": ["val"], "alias": "PPL_FORCE"},
    "tb_pre_leveller_pyro_meter_load_after":{"time_col": "ts", "value_cols": ["val"], "alias": "PPL_PYRO_AFTER"},
    "tb_ufc_temp_meas":                     {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_TEMP"},
    "tb_ufc_water_press_set":               {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_WATER_PRESS"},
    "tb_ufc_spiny_water_ratio":             {"time_col": "ts", "value_cols": ["val"], "alias": "UFC_SPINY_WATER_RATIO"},
    "tb_acc_pyro_meter_load_before":        {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_PYRO_BEFORE"},
    "tb_acc_pyro_meter_load_after":         {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_PYRO_AFTER"},
    "tb_acc_water_press_set":               {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_WATER_PRESS"},
    "tb_acc_spiny_water_ratio":             {"time_col": "ts", "value_cols": ["val"], "alias": "ACC_SPINY_WATER_RATIO"},
    "tb_hot_leveller_temp_entry":           {"time_col": "ts", "value_cols": ["val"], "alias": "HPL_TEMP_ENTRY"},
    "tb_desc_water_press_entry":            {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_WATER_PRESS_ENTRY"},
    "tb_desc_water_press_exit":             {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_WATER_PRESS_EXIT"},
    "tb_desc_pyro_meter_load_after":        {"time_col": "ts", "value_cols": ["val"], "alias": "DESC_PYRO_AFTER"},
}

# 所有 alias 列的有序列表（用于最终补全列）
ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]

# 固定的最终列顺序
ORDERED_PROCESS_COLS = [
    cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()
]

TIME_PARSE_KWARGS = {"errors": "coerce"}
BATCH_SIZE = 500


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
    df = df.sort_values(time_col).reset_index(drop=True)
    return df


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

    time_idx = df_proc.columns.get_loc(time_col)
    value_idx = df_proc.columns.get_loc(actual_col)

    all_dfs = []
    for start, end, slab_id in zip(starts, ends, slab_ids):
        left = np.searchsorted(times, start, side='left')
        # 修复1：end 侧改为 side='left'，不含 END_TIME 边界秒
        # 避免相邻工序共享同一边界秒导致重复行
        right = np.searchsorted(times, end, side='left')
        if left >= right:
            continue
        df_win = df_proc.iloc[left:right, [time_idx, value_idx]].copy()
        df_win["SLAB_ID"] = slab_id
        df_win["PROCEDURE_NAME"] = procedure
        df_win = df_win.rename(columns={time_col: "unified_time", actual_col: alias})
        all_dfs.append(df_win)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def align_and_merge_procedure_data(
    dfs_by_file: List[pd.DataFrame],
) -> pd.DataFrame:
    """
    将同一工序下多个文件的数据合并。
    修复2：concat 后先对所有 alias 列统一 fillna(0)，再 groupby 去重，
    保证不出现因稀疏 concat 导致的空值。
    """
    if not dfs_by_file:
        return pd.DataFrame()

    # 收集本次合并涉及的所有 alias 列
    all_process_cols = set()
    for df in dfs_by_file:
        for col in df.columns:
            if col not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]:
                all_process_cols.add(col)

    df_merged = pd.concat(dfs_by_file, ignore_index=True)
    df_merged = df_merged[df_merged["unified_time"].notna()].copy()

    if df_merged.empty:
        return pd.DataFrame()

    # 补全缺失列并立即 fillna(0)，避免 groupby 后出现 NaN
    for col in all_process_cols:
        if col not in df_merged.columns:
            df_merged[col] = 0.0
    df_merged[list(all_process_cols)] = df_merged[list(all_process_cols)].fillna(0)

    # 修复1：去重时不把 PROCEDURE_NAME 作为 key，
    # 同一 SLAB_ID + unified_time 无论来自哪个工序都合并成一行
    # 先保存 PROCEDURE_NAME 映射（取每个时间戳最早出现的工序名，供调试用）
    proc_map = (
        df_merged[["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
        .drop_duplicates(subset=["SLAB_ID", "unified_time"], keep="first")
    )

    df_merged = (
        df_merged
        .groupby(["SLAB_ID", "unified_time"], as_index=False)[list(all_process_cols)]
        .max()
    )

    # 把 PROCEDURE_NAME 重新 join 回来
    df_merged = df_merged.merge(proc_map, on=["SLAB_ID", "unified_time"], how="left")

    df_merged = df_merged.sort_values(["SLAB_ID", "unified_time"])
    return df_merged


# def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality, discovered_files):
#     batch_results = []
#
#     for slab_id in slab_ids_batch:
#         start_time = time.time()
#
#         df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id].copy()
#
#         if df_oper_slab.empty:
#             continue
#
#         # ===============================
#         # ✅ 只检查“同一工序内部”是否时间重叠
#         # ===============================
#         for proc, df_proc in df_oper_slab.groupby("PROCEDURE_NAME"):
#             df_proc = df_proc.sort_values("START_TIME")
#             overlap_mask = df_proc["START_TIME"].shift(-1) < df_proc["END_TIME"]
#
#             # 只有同一工序内部重叠才提示一次
#             if overlap_mask.any():
#                 print(f"[数据异常] SLAB_ID={slab_id} 工序={proc} 存在时间重叠")
#                 break  # 每个 slab 只提示一次即可
#
#         slab_all_files = []
#
#         for proc in df_oper_slab["PROCEDURE_NAME"].unique():
#             df_oper_proc = df_oper_slab[df_oper_slab["PROCEDURE_NAME"] == proc]
#
#             for filename in discovered_files.get(proc, []):
#                 file_cfg = get_file_config(filename)
#                 if not file_cfg or filename not in process_data_cache:
#                     continue
#
#                 df_ts = extract_timesteps_by_time_window(
#                     process_data_cache[filename],
#                     df_oper_proc,
#                     file_cfg,
#                     proc,
#                     filename,
#                 )
#
#                 if not df_ts.empty:
#                     slab_all_files.append(df_ts)
#
#         if not slab_all_files:
#             continue
#
#         df_slab = pd.concat(slab_all_files, ignore_index=True)
#         df_slab = df_slab[df_slab["unified_time"].notna()].copy()
#
#         # ===============================
#         # ✅ 强制时间类型
#         # ===============================
#         df_slab["unified_time"] = pd.to_datetime(df_slab["unified_time"])
#
#         # ===============================
#         # ✅ 严格时间排序
#         # ===============================
#         df_slab = df_slab.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)
#
#         alias_cols = [
#             c for c in df_slab.columns
#             if c not in ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
#         ]
#
#         for col in alias_cols:
#             df_slab[col] = df_slab[col].fillna(0)
#
#         # ===============================
#         # ✅ 聚合（同一秒只保留一行）
#         # ===============================
#         proc_map = (
#             df_slab
#             .drop_duplicates(subset=["SLAB_ID", "unified_time"], keep="last")
#             [["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
#         )
#
#         df_slab = (
#             df_slab
#             .groupby(["SLAB_ID", "unified_time"], as_index=False)[alias_cols]
#             .last()
#         )
#
#         df_slab = df_slab.merge(
#             proc_map,
#             on=["SLAB_ID", "unified_time"],
#             how="left"
#         )
#
#         # 再排序一次，保证绝对单调
#         df_slab = df_slab.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)
#
#         batch_results.append(df_slab)
#
#         print(f"完成 SLAB_ID={slab_id} 用时 {time.time()-start_time:.2f}s")
#
#     if not batch_results:
#         return pd.DataFrame()
#
#     df_batch = pd.concat(batch_results, ignore_index=True)
#
#     # ===============================
#     # ✅ 全局时间排序
#     # ===============================
#     df_batch["unified_time"] = pd.to_datetime(df_batch["unified_time"])
#     df_batch = df_batch.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)
#
#     # ===============================
#     # ✅ 合并质量表
#     # ===============================
#     df_batch = df_batch.merge(df_quality, on="SLAB_ID", how="left")
#
#     # ===============================
#     # ✅ 目标变量过滤
#     # ===============================
#     target_cols = ["TS", "YIELD_REH", "YIELD_REL", "HOMO_EL", "YIELD_RATE", "IMPACT_AVG"]
#     existing_targets = [c for c in target_cols if c in df_batch.columns]
#
#     if existing_targets:
#         mask_na = df_batch[existing_targets].isna().all(axis=1)
#         mask_zero = (df_batch[existing_targets].fillna(0) == 0).all(axis=1)
#         df_batch = df_batch[~(mask_na | mask_zero)].copy()
#
#     # ===============================
#     # ✅ 补齐所有 alias 列
#     # ===============================
#     for col in ALL_ALIASES:
#         if col not in df_batch.columns:
#             df_batch[col] = 0.0
#         else:
#             df_batch[col] = df_batch[col].fillna(0)
#
#     meta_cols = ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
#     ordered_cols = meta_cols + ALL_ALIASES
#     other_cols = [c for c in df_batch.columns if c not in ordered_cols]
#
#     df_batch = df_batch[ordered_cols + other_cols]
#
#     return df_batch




# ===============================
# process_batch 修正版
# ===============================
def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality, discovered_files):
    batch_results = []

    for slab_id in slab_ids_batch:
        start_time = time.time()

        df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id].copy()
        if df_oper_slab.empty:
            continue

        # ===============================
        # 仅检查同一工序内部重叠
        # ===============================
        for proc, df_proc in df_oper_slab.groupby("PROCEDURE_NAME"):
            df_proc = df_proc.sort_values("START_TIME")
            overlap_mask = df_proc["START_TIME"].shift(-1) < df_proc["END_TIME"]
            if overlap_mask.any():
                print(f"[数据异常] SLAB_ID={slab_id} 工序={proc} 存在时间重叠")
                break  # 每个 slab 只提示一次

        slab_all_files = []

        for proc in df_oper_slab["PROCEDURE_NAME"].unique():
            df_oper_proc = df_oper_slab[df_oper_slab["PROCEDURE_NAME"] == proc]

            for filename in discovered_files.get(proc, []):
                if filename not in process_data_cache:
                    continue
                file_cfg = get_file_config(filename)
                if not file_cfg:
                    continue

                df_ts = extract_timesteps_by_time_window(
                    process_data_cache[filename],
                    df_oper_proc,
                    file_cfg,
                    proc,
                    filename
                )
                if not df_ts.empty:
                    slab_all_files.append(df_ts)

        if not slab_all_files:
            continue

        # ===============================
        # 合并所有文件数据，确保同一秒所有 alias 列在一行
        # ===============================
        df_slab = pd.concat(slab_all_files, ignore_index=True)

        # fillna(0) 保证聚合不会丢数据
        alias_cols = [c for c in df_slab.columns if c not in ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
        for col in alias_cols:
            df_slab[col] = df_slab[col].fillna(0)

        # 聚合：同一秒同一 SLAB_ID 所有列最大值合并
        df_slab = df_slab.groupby(["SLAB_ID", "unified_time"], as_index=False).max()

        # 重新 attach PROCEDURE_NAME，取最早出现的一个
        proc_map = (
            slab_all_files[0][["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
            .drop_duplicates(subset=["SLAB_ID", "unified_time"], keep="first")
        )
        df_slab = df_slab.merge(proc_map, on=["SLAB_ID", "unified_time"], how="left")

        # 补全所有 alias 列
        for col in ALL_ALIASES:
            if col not in df_slab.columns:
                df_slab[col] = 0.0

        # 严格排序
        df_slab = df_slab.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)

        batch_results.append(df_slab)
        print(f"完成 SLAB_ID={slab_id} 用时 {time.time() - start_time:.2f}s")

    if not batch_results:
        return pd.DataFrame()

    df_batch = pd.concat(batch_results, ignore_index=True)
    df_batch = df_batch.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)

    # 合并质量表
    df_batch = df_batch.merge(df_quality, on="SLAB_ID", how="left")

    return df_batch


def main():
    print("========== 开始处理 ==========")

    # ===============================
    # 1️⃣ 读取基础表
    # ===============================
    df_quality, df_oper = load_base_tables()

    print("\n每个 SLAB_ID 的工序数量分布：")
    print(df_oper.groupby("SLAB_ID").size().value_counts().sort_index())

    print("\n工序数量最多的前5个 SLAB_ID：")
    print(df_oper.groupby("SLAB_ID").size().nlargest(5))

    # ===============================
    # 2️⃣ 自动发现 CSV
    # ===============================
    discovered_files = discover_csv_files()

    # ===============================
    # 3️⃣ 预加载过程数据
    # ===============================
    process_data_cache: Dict[str, pd.DataFrame] = {}
    all_files = set().union(*discovered_files.values())

    print("\n预加载过程数据表...")
    for fn in all_files:
        try:
            process_data_cache[fn] = load_process_data(fn)
        except Exception as e:
            print(f"读取 {fn} 失败：{e}，跳过")

    unique_slab_ids = df_oper["SLAB_ID"].unique()
    print(f"\n总 SLAB_ID 数量: {len(unique_slab_ids)}")

    # ===============================
    # 4️⃣ 输出文件初始化
    # ===============================
    out_path = os.path.join(OUTPUT_DIR, "process_timeseries_clean.csv")
    if os.path.exists(out_path):
        os.remove(out_path)

    total_processed = 0
    first_write = True

    # ===============================
    # 5️⃣ 分批处理（改为直接写文件，避免 all_results 占内存）
    # ===============================
    for i in range(0, len(unique_slab_ids), BATCH_SIZE):
        batch = unique_slab_ids[i:i + BATCH_SIZE]
        print(f"\n处理批次 {i // BATCH_SIZE + 1}，{len(batch)} 个 SLAB_ID")

        df_batch = process_batch(
            batch,
            df_oper,
            process_data_cache,
            df_quality,
            discovered_files
        )

        if df_batch.empty:
            print("  本批次无有效数据")
            continue

        # ===== 再次保证 alias 列完整 =====
        for col in ALL_ALIASES:
            if col not in df_batch.columns:
                df_batch[col] = 0.0
            else:
                df_batch[col] = df_batch[col].fillna(0)

        # ===== 全局排序 =====
        df_batch = df_batch.sort_values(["SLAB_ID", "unified_time"])

        # ===== 写入文件（分批追加）=====
        df_batch.to_csv(
            out_path,
            mode='w' if first_write else 'a',
            header=first_write,
            index=False,
            encoding="utf-8-sig"
        )

        first_write = False
        total_processed += len(df_batch)

        print(f"  写入 {len(df_batch)} 行，累计 {total_processed}")

        # 主动释放内存
        del df_batch

    # ===============================
    # 6️⃣ 处理完成统计
    # ===============================
    if os.path.exists(out_path):
        print("\n========== 处理完成 ==========")
        print(f"输出文件: {out_path}")

        df_check = pd.read_csv(out_path)

        print(f"总行数: {len(df_check)}")
        print(f"列数: {df_check.shape[1]}")
        print(f"唯一 SLAB_ID: {df_check['SLAB_ID'].nunique()}")

        if "PROCEDURE_NAME" in df_check.columns:
            print(f"唯一工序: {sorted(df_check['PROCEDURE_NAME'].dropna().unique())}")

        print("\n=== 数据质量验证 ===")

        dup = df_check.duplicated(subset=["SLAB_ID", "unified_time"]).sum()
        print(f"重复 (SLAB_ID + unified_time) 行数: {dup}  {'✓' if dup == 0 else '✗ 仍有重复'}")

        alias_null = {
            col: df_check[col].isna().sum()
            for col in ALL_ALIASES if col in df_check.columns
        }

        null_cols = {k: v for k, v in alias_null.items() if v > 0}
        if null_cols:
            print(f"仍含空值的 alias 列: {null_cols}")
        else:
            print("所有 alias 列无空值  ✓")

    else:
        print("\n所有批次均无有效数据，未生成输出文件")

    print("\n处理结束。")

if __name__ == "__main__":
    main()