import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

"""
时序数据合表 - 修复版（含运行时间统计）
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

ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]
ORDERED_PROCESS_COLS = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]

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


# def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality, discovered_files):
#     batch_results = []
#     batch_start = time.time()
#     slab_times = []
#
#     for slab_id in slab_ids_batch:
#         slab_start = time.time()
#
#         df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id].copy()
#         if df_oper_slab.empty:
#             continue
#
#         # 检查同一工序内部时间重叠
#         for proc, df_proc in df_oper_slab.groupby("PROCEDURE_NAME"):
#             df_proc = df_proc.sort_values("START_TIME")
#             overlap_mask = df_proc["START_TIME"].shift(-1) < df_proc["END_TIME"]
#             if overlap_mask.any():
#                 print(f"[数据异常] SLAB_ID={slab_id} 工序={proc} 存在时间重叠")
#                 break
#
#         slab_all_files = []
#
#         for proc in df_oper_slab["PROCEDURE_NAME"].unique():
#             df_oper_proc = df_oper_slab[df_oper_slab["PROCEDURE_NAME"] == proc]
#
#             for filename in discovered_files.get(proc, []):
#                 if filename not in process_data_cache:
#                     continue
#                 file_cfg = get_file_config(filename)
#                 if not file_cfg:
#                     continue
#
#                 df_ts = extract_timesteps_by_time_window(
#                     process_data_cache[filename],
#                     df_oper_proc,
#                     file_cfg,
#                     proc,
#                     filename
#                 )
#                 if not df_ts.empty:
#                     slab_all_files.append(df_ts)
#
#         if not slab_all_files:
#             continue
#
#         df_slab = pd.concat(slab_all_files, ignore_index=True)
#
#         alias_cols = [c for c in df_slab.columns if c not in ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
#         for col in alias_cols:
#             df_slab[col] = df_slab[col].fillna(0)
#
#         df_slab = df_slab.groupby(["SLAB_ID", "unified_time"], as_index=False).max()
#
#         # 取最早的 PROCEDURE_NAME
#         proc_map = (
#             slab_all_files[0][["SLAB_ID", "unified_time", "PROCEDURE_NAME"]]
#             .drop_duplicates(subset=["SLAB_ID", "unified_time"], keep="first")
#         )
#         df_slab = df_slab.merge(proc_map, on=["SLAB_ID", "unified_time"], how="left")
#
#         for col in ALL_ALIASES:
#             if col not in df_slab.columns:
#                 df_slab[col] = 0.0
#
#         df_slab = df_slab.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)
#
#         batch_results.append(df_slab)
#
#         slab_time = time.time() - slab_start
#         slab_times.append(slab_time)
#         print(f"完成 SLAB_ID={slab_id} 用时 {slab_time:.2f}s")
#
#     if slab_times:
#         avg_slab_time = sum(slab_times) / len(slab_times)
#         print(f"  本批次 {len(slab_times)} 条 SLAB_ID 处理完成，"
#               f"平均 {avg_slab_time:.2f}s/条，总计 {time.time()-batch_start:.2f}s")
#
#     if not batch_results:
#         return pd.DataFrame()
#
#     df_batch = pd.concat(batch_results, ignore_index=True)
#     df_batch = df_batch.sort_values(["SLAB_ID", "unified_time"]).reset_index(drop=True)
#
#     df_batch = df_batch.merge(df_quality, on="SLAB_ID", how="left")
#
#     return df_batch

def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality, discovered_files):
    batch_results = []
    batch_start = time.time()
    slab_times = []

    for slab_id in slab_ids_batch:
        slab_start = time.time()
        df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id].copy()
        if df_oper_slab.empty:
            continue

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

        # 合并所有文件
        df_slab = pd.concat(slab_all_files, ignore_index=True)

        # 补齐所有 alias 列
        alias_cols = [c for c in ALL_ALIASES if c not in df_slab.columns]
        for col in alias_cols:
            df_slab[col] = 0.0

        # 同一 SLAB_ID + unified_time 合并，同一秒内数据取 max
        group_cols = ["SLAB_ID", "unified_time"]
        df_slab = df_slab.groupby(group_cols, as_index=False).agg({**{col: "max" for col in ALL_ALIASES},
                                                                    "PROCEDURE_NAME": "first"})

        # 保证列顺序：前三列 + alias
        df_slab = df_slab[["SLAB_ID", "unified_time", "PROCEDURE_NAME"] + ALL_ALIASES]

        batch_results.append(df_slab)

        slab_time = time.time() - slab_start
        slab_times.append(slab_time)
        print(f"完成 SLAB_ID={slab_id} 用时 {slab_time:.2f}s")

    if not batch_results:
        return pd.DataFrame()

    df_batch = pd.concat(batch_results, ignore_index=True)
    df_batch = df_batch.merge(df_quality, on="SLAB_ID", how="left")

    # 最终列顺序同样保证前三列 + alias + 其他质量列
    other_cols = [c for c in df_batch.columns if c not in ["SLAB_ID", "unified_time", "PROCEDURE_NAME"] + ALL_ALIASES]
    df_batch = df_batch[["SLAB_ID", "unified_time", "PROCEDURE_NAME"] + ALL_ALIASES + other_cols]

    return df_batch

def main():
    print("========== 开始处理 ==========")
    overall_start = time.time()

    # 阶段1: 读取基础表
    stage_start = time.time()
    df_quality, df_oper = load_base_tables()
    base_load_time = time.time() - stage_start
    print(f"读取质量表 & 工序时间表 耗时: {base_load_time:.2f} 秒\n")

    print("每个 SLAB_ID 的工序数量分布：")
    print(df_oper.groupby("SLAB_ID").size().value_counts().sort_index())

    print("\n工序数量最多的前5个 SLAB_ID：")
    print(df_oper.groupby("SLAB_ID").size().nlargest(5))

    # 阶段2: 发现文件
    stage_start = time.time()
    discovered_files = discover_csv_files()
    discover_time = time.time() - stage_start
    print(f"\n发现文件耗时: {discover_time:.2f} 秒")

    # 阶段3: 预加载过程数据
    stage_start = time.time()
    process_data_cache: Dict[str, pd.DataFrame] = {}
    all_files = set().union(*discovered_files.values())

    print("\n预加载过程数据表...")
    for fn in all_files:
        try:
            process_data_cache[fn] = load_process_data(fn)
        except Exception as e:
            print(f"读取 {fn} 失败：{e}，跳过")
    preload_time = time.time() - stage_start
    print(f"预加载所有过程数据表总耗时: {preload_time:.2f} 秒\n")

    unique_slab_ids = df_oper["SLAB_ID"].unique()
    print(f"总 SLAB_ID 数量: {len(unique_slab_ids)}")

    out_path = os.path.join(OUTPUT_DIR, "process_timeseries_clean.csv")
    if os.path.exists(out_path):
        os.remove(out_path)

    total_processed = 0
    first_write = True
    batch_total_start = time.time()

    # 分批处理
    for i in range(0, len(unique_slab_ids), BATCH_SIZE):
        batch_start = time.time()
        batch = unique_slab_ids[i:i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        total_batches = (len(unique_slab_ids) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n处理批次 {batch_idx}/{total_batches}，{len(batch)} 个 SLAB_ID")

        df_batch = process_batch(
            batch, df_oper, process_data_cache, df_quality, discovered_files
        )

        batch_time = time.time() - batch_start

        if df_batch.empty:
            print(f"  本批次无有效数据（耗时 {batch_time:.2f} 秒）")
            continue

        # 补齐所有 alias 列
        for col in ALL_ALIASES:
            if col not in df_batch.columns:
                df_batch[col] = 0.0
            else:
                df_batch[col] = df_batch[col].fillna(0)

        df_batch = df_batch.sort_values(["SLAB_ID", "unified_time"])

        # 写入
        df_batch.to_csv(
            out_path,
            mode='w' if first_write else 'a',
            header=first_write,
            index=False,
            encoding="utf-8-sig"
        )
        first_write = False

        rows_this_batch = len(df_batch)
        total_processed += rows_this_batch

        print(f"  写入 {rows_this_batch:,} 行，批次耗时 {batch_time:.2f} 秒，累计 {total_processed:,} 行")

        del df_batch

    batch_total_time = time.time() - batch_total_start
    total_time = time.time() - overall_start

    print("\n" + "="*60)
    print("处理完成！运行时间总结")
    print("-"*60)
    print(f"总耗时           : {total_time:>10.2f} 秒  ≈ {total_time/60:>6.1f} 分钟")
    print(f"  预加载数据     : {preload_time:>10.2f} 秒  ({preload_time/total_time*100:>5.1f}%)")
    print(f"  所有批次处理   : {batch_total_time:>10.2f} 秒  ({batch_total_time/total_time*100:>5.1f}%)")
    print(f"  读取基础表     : {base_load_time:>10.2f} 秒  ({base_load_time/total_time*100:>5.1f}%)")
    print(f"  发现文件       : {discover_time:>10.2f} 秒  ({discover_time/total_time*100:>5.1f}%)")
    print("-"*60)
    print(f"最终输出文件     : {out_path}")
    print(f"总写入行数       : {total_processed:,} 行")
    print("="*60)

    # 最终文件验证（可选）
    if os.path.exists(out_path):
        print("\n最终文件简单验证：")
        df_check = pd.read_csv(out_path, nrows=0)  # 只读表头
        print(f"列数: {df_check.shape[1]}")
        print(f"已写入文件大小约: {os.path.getsize(out_path) / 1024 / 1024:.1f} MB")

    print("\n处理结束。")


if __name__ == "__main__":
    main()