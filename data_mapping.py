import os
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

"""
根据说明，实现"时序数据"版本的映射（内存优化版）：

1. 质量表 select_v_quality_all.xlsx
   - 关键字段：FUR_EXIT_SLAB_ID （与工序时间表中的 SLAB_ID 对应）

2. 工序时间表 v_jk_oper_time.xlsx
   - 字段：SLAB_ID, PROCEDURE_NAME, START_TIME, END_TIME
   - PROCEDURE_NAME 取值示例：RM, FM, PPL, UFC, ACC, HPL 等
 
3. 多个工序过程数据表（Excel），例如：
   - ACC出口温度.xlsx
   - ACC入口温度.xlsx
   - 除鳞出口压力.xlsx
   - 除鳞入口压力.xlsx
   - 粗轧实际轧制速度.xlsx
   - 精轧机轧制速度.xlsx
   - 精轧测量轧制力.xlsx
   - 精轧测量轧制速度.xlsx
   - 精轧后温度.xlsx
   - 预矫直高度.xlsx

   同一个工序（例如 FM 精轧）可以对应多个过程数据表。
   这些过程表一般具有类似字段，如：start_time, avg_speed，
   每一行是一个时间步的数据。

4. 需求（时序）：
   - 按 SLAB_ID 关联质量表和工序时间表。
   - 对于每个 SLAB_ID、每个工序的时间区间 [START_TIME, END_TIME]，
     从对应的过程数据表中筛选该时间窗口内"所有"时间步数据（不聚合）。
   - 将筛选出的过程数据逐行保留（即每一行仍是一个时间步），
     并为每一行补齐该 SLAB_ID 在质量表中的所有静态特征，
     形成适合 LSTM 训练的长表格式：一行 = 一个时间步。
   - 某些材料没有经过某些工序，或某工序在某些表中没有数据时，
     仍然为该材料/工序/表生成至少一行记录，其过程量字段填充为 -1，
     以表示"该时间序列缺失/未经过此过程"。

使用说明：
   1. 根据你真实的 Excel 列名，修改 PROCESS_COLUMNS_CONFIG 中各文件的配置：
      - time_col：时间列名（如 start_time）
      - slab_id_col：钢板 ID 列名（若没有可设为 None）
      - value_cols：时间步上的过程量列名列表（如 ["avg_speed"]）
      - alias：希望在 SOURCE_FILE 列中使用的简短别名
   2. 如有新的过程数据表，在 PROCEDURE_FILES 和 PROCESS_COLUMNS_CONFIG 中补充即可。
"""

BASE_DIR = 'E:/工作材料/2025/时序模型材料/京唐/data'

# -----------------------------
# 文件名配置
# -----------------------------

QUALITY_FILE = os.path.join(BASE_DIR, "select_v_quality_all.xlsx")
OPER_TIME_FILE = os.path.join(BASE_DIR, "v_jk_oper_time.xlsx")

# 工序英文缩写 -> 对应的多个过程数据文件名列表
# 这里只是示例绑定，请根据你的实际情况检查和调整。
PROCEDURE_FILES: Dict[str, List[str]] = {
    "RM": [  # 粗轧
        "粗轧实际轧制速度.xlsx",
        "粗轧轧制力.xlsx",

    ],
    "FM": [  # 精轧
        "精轧前温度.xlsx",
        "精轧测量轧制力.xlsx",
        "精轧测量轧制速度.xlsx",
        "精轧后温度.xlsx",
        "精轧轧制测量出口操作侧厚度.xlsx",
        "精轧轧制测量出口传动侧厚度.xlsx"
    ],
    "PPL": [  # 预矫直机
        "预矫直后温度.xlsx",
    ],
    "UFC": [  # 超快冷
        # 示例：如果有 UFC 相关表格，在这里添加
        # "UFC出口温度.xlsx",
    ],
    "ACC": [  # 层冷
        "ACC出口温度.xlsx",
        "ACC入口温度.xlsx",
    ],
    "HPL": [  # 热矫直机
        # 示例：如果有 HPL 相关表格，在这里添加
        # "热矫直机温度.xlsx",
    ],
    # 如果"除鳞"属于某个具体工序（如 RM），可以将它的表文件名加到对应列表中：
    # 例如： "RM": ["粗轧实际轧制速度.xlsx", "除鳞出口压力.xlsx", "除鳞入口压力.xlsx"]
}

# 每个过程数据文件的列配置
# !!! 请务必根据实际 Excel 的列名进行修改 !!!
#
# 典型过程表字段示例：start_time, avg_speed
#   - time_col   -> "start_time"
#   - value_cols -> ["avg_speed"]
#
PROCESS_COLUMNS_CONFIG: Dict[str, Dict] = {
    # --------------- ACC 相关示例 ---------------
    "ACC出口温度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "ACC_EXIT_TEMP ",
    },
    "ACC入口温度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "ACC_ENTRY_TEMP",
    },
    # --------------- 精轧相关示例 ---------------
    "精轧前温度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_PRE_TEMP",
    },
    "精轧测量轧制力.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_FORCE",
    },
    "精轧测量轧制速度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_MEAS_SPEED",
    },
    "精轧后温度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_AFTER_TEMP",
    },
    "精轧轧制测量出口操作侧厚度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_EXIT_OPER_THICK",
    },
    "精轧轧制测量出口传动侧厚度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "FM_EXIT_DRIVE_THICK",
    },
    # --------------- 粗轧 / 预矫直 等示例 ---------------
    "粗轧实际轧制速度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "RM_SPEED",
    },
    "粗轧轧制力.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "RM_FORCE",
    },
    "预矫直后温度.xlsx": {
        "time_col": "start_time",
        "slab_id_col": None,
        "value_cols": ["avg_speed"],
        "alias": "PPL_HEIGHT",
    },
}

# 时间解析配置：确保时间精度到秒级
# 如果 Excel 时间已经是 datetime 类型，可以保持默认；
# 如果是字符串，可以在这里指定 format，如：
# TIME_PARSE_KWARGS = {"format": "%Y-%m-%d %H:%M:%S", "errors": "coerce"}
TIME_PARSE_KWARGS = {"errors": "coerce"}

# 时间对齐的粒度（秒级对齐，用于合并同一工序不同过程表的数据）
TIME_ALIGNMENT_FREQ = "1S"  # 1秒对齐

# 分批处理参数
BATCH_SIZE = 500  # 减小批次大小以降低内存压力
CHUNK_SIZE = 10000  # 写入CSV时的分块大小


def load_base_tables():
    """读取质量表和工序时间表，并做基本字段统一。"""
    print("读取质量表和工序时间表 ...")

    df_quality = pd.read_excel(QUALITY_FILE)
    df_oper = pd.read_excel(OPER_TIME_FILE)

    # 统一 SLAB_ID 名称
    if "SLAB_ID" not in df_quality.columns:
        if "FUR_EXIT_SLAB_ID" in df_quality.columns:
            df_quality = df_quality.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
        else:
            raise ValueError(
                "质量表中找不到 'FUR_EXIT_SLAB_ID' 或 'SLAB_ID' 字段，请检查 select_v_quality_all.xlsx 列名。"
            )

    # 校验工序时间表必须字段
    required_cols = ["SLAB_ID", "PROCEDURE_NAME", "START_TIME", "END_TIME"]
    for col in required_cols:
        if col not in df_oper.columns:
            raise ValueError(f"v_jk_oper_time.xlsx 缺少必要列: {col}")

    # 转换时间列
    df_oper["START_TIME"] = pd.to_datetime(df_oper["START_TIME"], **TIME_PARSE_KWARGS)
    df_oper["END_TIME"] = pd.to_datetime(df_oper["END_TIME"], **TIME_PARSE_KWARGS)

    # 只保留我们关心的工序
    target_procs = list(PROCEDURE_FILES.keys())
    df_oper = df_oper[df_oper["PROCEDURE_NAME"].isin(target_procs)].copy()

    # 排序方便查看
    df_oper = df_oper.sort_values(["SLAB_ID", "PROCEDURE_NAME", "START_TIME"])

    return df_quality, df_oper


def load_process_data(filename: str) -> pd.DataFrame:
    """
    读取单个过程数据文件，并按配置进行时间列解析。
    确保时间精度保持到秒级。
    """
    cfg = PROCESS_COLUMNS_CONFIG.get(filename)
    if cfg is None:
        raise ValueError(f"PROCESS_COLUMNS_CONFIG 中没有找到文件配置：{filename}")

    path = os.path.join(BASE_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到过程数据文件：{path}")

    print(f"读取过程数据表：{filename}")
    df = pd.read_excel(path)

    time_col = cfg["time_col"]
    if time_col not in df.columns:
        raise ValueError(f"{filename} 中找不到时间列 '{time_col}'，请检查 PROCESS_COLUMNS_CONFIG。")

    # 转换时间列，确保精度到秒级
    df[time_col] = pd.to_datetime(df[time_col], **TIME_PARSE_KWARGS)
    # 如果时间列有微秒或纳秒，向下取整到秒级
    if df[time_col].dtype == "datetime64[ns]":
        df[time_col] = df[time_col].dt.floor("1S")

    df = df.dropna(subset=[time_col])

    return df


def extract_timesteps_by_time_window(
        df_proc: pd.DataFrame,
        df_oper_proc: pd.DataFrame,
        file_cfg: Dict,
        procedure: str,
        filename: str,
) -> pd.DataFrame:
    """
    将某个工序的某个过程数据表，与该工序对应的时间窗口进行匹配，
    返回"所有时间步"的长表数据：
        每一行 = 某块板在某工序某表中的一个时间步。

    输出至少包含列：
        - SLAB_ID
        - PROCEDURE_NAME
        - time_col      （例如 start_time，统一命名为 unified_time）
        - value_cols    （例如 avg_speed，带前缀以区分来源）
    """
    time_col: str = file_cfg["time_col"]
    slab_id_col: Optional[str] = file_cfg.get("slab_id_col")
    value_cols: List[str] = file_cfg["value_cols"]
    alias: str = file_cfg.get("alias", os.path.splitext(filename)[0])

    all_rows: List[pd.DataFrame] = []

    for _, op in df_oper_proc.iterrows():
        slab_id = op["SLAB_ID"]
        start = op["START_TIME"]
        end = op["END_TIME"]

        # 先按 SLAB_ID 过滤（如果过程表中有此列）
        if slab_id_col and slab_id_col in df_proc.columns:
            df_tmp = df_proc[df_proc[slab_id_col] == slab_id]
        else:
            df_tmp = df_proc

        # 再按时间窗口过滤
        # 注意：Excel中实际列名是 value_cols（通常是 "avg_speed"），但输出时要重命名为 alias 的值
        mask = (df_tmp[time_col] >= start) & (df_tmp[time_col] <= end)
        # 从Excel中读取实际列名（通常是 "avg_speed"）
        actual_col_in_excel = None
        for vc in value_cols:
            if vc in df_tmp.columns:
                actual_col_in_excel = vc
                break
        if actual_col_in_excel is None:
            raise ValueError(f"在 {filename} 中找不到列 {value_cols}，请检查Excel文件列名。")

        cols_keep = [time_col] + [actual_col_in_excel]
        df_window = df_tmp.loc[mask, cols_keep].copy()

        if df_window.empty:
            # 没有任何时间步：为该窗口至少生成一行"占位"记录，数值填 -1，时间为空
            empty_row = {
                "SLAB_ID": slab_id,
                "PROCEDURE_NAME": procedure,
                "unified_time": pd.NaT,
            }
            # 使用 alias 作为列名（去掉空格）
            alias_clean = alias.strip()
            empty_row[alias_clean] = -1
            all_rows.append(pd.DataFrame([empty_row]))
        else:
            df_window = df_window.copy()
            df_window["SLAB_ID"] = slab_id
            df_window["PROCEDURE_NAME"] = procedure
            # 统一时间列名为 unified_time
            df_window = df_window.rename(columns={time_col: "unified_time"})
            # 将Excel中的列名（如 "avg_speed"）重命名为 alias 的值
            alias_clean = alias.strip()
            df_window = df_window.rename(columns={actual_col_in_excel: alias_clean})
            all_rows.append(df_window)

    if not all_rows:
        return pd.DataFrame(columns=["SLAB_ID", "PROCEDURE_NAME", "unified_time"])

    return pd.concat(all_rows, ignore_index=True)


def align_and_merge_procedure_data(
        dfs_by_file: List[pd.DataFrame], procedure: str, slab_id: str
) -> pd.DataFrame:
    """
    对于同一个SLAB_ID+PROCEDURE_NAME组合，将来自不同过程数据表的数据按时间对齐合并。

    参数:
        dfs_by_file: 来自同一工序不同过程表的DataFrame列表
        procedure: 工序名称
        slab_id: 钢板ID

    返回:
        按时间对齐合并后的DataFrame，每个时间步一行，包含所有过程表的变量
    """
    if not dfs_by_file:
        return pd.DataFrame(columns=["SLAB_ID", "PROCEDURE_NAME", "unified_time"])

    # 收集所有过程表的列名（除了SLAB_ID, PROCEDURE_NAME, unified_time）
    all_process_cols = set()
    for df in dfs_by_file:
        for col in df.columns:
            if col not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]:
                all_process_cols.add(col)

    # 合并所有过程表的数据（可能有重复时间）
    df_merged = pd.concat(dfs_by_file, ignore_index=True)

    # 过滤掉时间为NaT的行（这些是占位记录，后面单独处理）
    df_valid = df_merged[df_merged["unified_time"].notna()].copy()
    df_na = df_merged[df_merged["unified_time"].isna()].copy()

    if df_valid.empty:
        # 如果所有数据都是占位记录，直接返回
        return df_merged

    # 按时间排序
    df_valid = df_valid.sort_values("unified_time")

    # 按时间对齐：将相同或接近的时间步合并到一行
    # 使用1秒对齐，将时间向下取整到秒级
    df_valid["time_aligned"] = df_valid["unified_time"].dt.floor("1S")

    # 按对齐后的时间分组，对数值列取平均值（如果同一秒有多条记录）
    group_cols = ["SLAB_ID", "PROCEDURE_NAME", "time_aligned"]
    value_cols = [c for c in df_valid.columns if c not in group_cols + ["unified_time"]]

    if value_cols:
        agg_dict = {}
        for c in value_cols:
            if df_valid[c].dtype in ["float64", "int64", "float32", "int32"]:
                agg_dict[c] = "mean"
            else:
                agg_dict[c] = "first"
        df_aligned = df_valid.groupby(group_cols, as_index=False).agg(agg_dict)
    else:
        df_aligned = df_valid.groupby(group_cols, as_index=False).first()

    # 确保所有过程表的列都出现在结果中（如果某个时间步没有数据，会是NaN，后续会填充为-1）
    for col in all_process_cols:
        if col not in df_aligned.columns:
            df_aligned[col] = pd.NA

    # 重命名回 unified_time
    df_aligned = df_aligned.rename(columns={"time_aligned": "unified_time"})

    # 如果有NaT占位记录，也添加回去（但通常不应该有，因为我们已经为每个文件单独处理了）
    if not df_na.empty:
        df_aligned = pd.concat([df_aligned, df_na], ignore_index=True)

    return df_aligned


def process_batch(slab_ids_batch, df_oper, process_data_cache, df_quality):
    """处理一批SLAB_ID的时序数据"""
    all_sequences: List[pd.DataFrame] = []

    # 对于该批次的每个SLAB_ID，收集所有工序的数据
    for slab_id in slab_ids_batch:
        # 获取该SLAB_ID涉及的所有工序
        df_oper_slab = df_oper[df_oper["SLAB_ID"] == slab_id].copy()

        for proc in df_oper_slab["PROCEDURE_NAME"].unique():
            df_oper_proc = df_oper_slab[df_oper_slab["PROCEDURE_NAME"] == proc].copy()

            if df_oper_proc.empty:
                continue

            dfs_by_file: List[pd.DataFrame] = []

            # 遍历该工序的所有过程数据表
            for filename in PROCEDURE_FILES.get(proc, []):
                if filename not in PROCESS_COLUMNS_CONFIG:
                    continue

                if filename not in process_data_cache:
                    continue

                df_proc = process_data_cache[filename]
                file_cfg = PROCESS_COLUMNS_CONFIG[filename]
                df_ts = extract_timesteps_by_time_window(
                    df_proc=df_proc,
                    df_oper_proc=df_oper_proc,
                    file_cfg=file_cfg,
                    procedure=proc,
                    filename=filename,
                )

                # 只保留当前SLAB_ID的数据
                df_ts = df_ts[df_ts["SLAB_ID"] == slab_id].copy()
                if not df_ts.empty:
                    dfs_by_file.append(df_ts)

            # 对齐并合并该SLAB_ID+PROCEDURE_NAME的所有过程表数据
            if dfs_by_file:
                df_aligned = align_and_merge_procedure_data(dfs_by_file, proc, slab_id)
                all_sequences.append(df_aligned)
            else:
                # 如果没有任何过程数据，至少生成一行占位记录
                empty_row = {
                    "SLAB_ID": slab_id,
                    "PROCEDURE_NAME": proc,
                    "unified_time": pd.NaT,
                }
                # 为所有可能的过程量列添加占位（-1），使用 alias 作为列名
                for filename in PROCEDURE_FILES.get(proc, []):
                    if filename in PROCESS_COLUMNS_CONFIG:
                        cfg = PROCESS_COLUMNS_CONFIG[filename]
                        alias = cfg.get("alias", os.path.splitext(filename)[0])
                        alias_clean = alias.strip()
                        empty_row[alias_clean] = -1
                all_sequences.append(pd.DataFrame([empty_row]))

    if not all_sequences:
        return pd.DataFrame()  # 返回空DataFrame

    # 合并该批次的所有序列
    df_batch_ts = pd.concat(all_sequences, ignore_index=True)

    # 按 SLAB_ID + PROCEDURE_NAME + 时间排序（便于LSTM训练时按组读取）
    df_batch_ts = df_batch_ts.sort_values(["SLAB_ID", "PROCEDURE_NAME", "unified_time"])

    # 把质量表信息补充到每一个时间步行上
    # 使用左连接，确保即使质量表中没有对应SLAB_ID也能保留时序数据
    df_batch_final = df_batch_ts.merge(df_quality, on="SLAB_ID", how="left")

    # 清理内存
    del df_batch_ts

    # 进一步清洗：如果六个目标变量 TS, YIELD_REH, YIELD_REL, HOMO_EL,
    # YIELD_RATE, IMPACT_AVG 全部为空，或全部为 0，则删除该行
    target_cols = ["TS", "YIELD_REH", "YIELD_REL", "HOMO_EL", "YIELD_RATE", "IMPACT_AVG"]
    existing_target_cols = [c for c in target_cols if c in df_batch_final.columns]

    if existing_target_cols:
        # 逐列检查，避免一次性创建大的子DataFrame
        mask_all_na = pd.Series([True] * len(df_batch_final), index=df_batch_final.index)
        mask_all_zero = pd.Series([True] * len(df_batch_final), index=df_batch_final.index)

        for col in existing_target_cols:
            col_series = df_batch_final[col]
            mask_all_na &= col_series.isna()
            filled_col = col_series.fillna(0)
            mask_all_zero &= (filled_col == 0)

        mask_drop = mask_all_na | mask_all_zero

        rows_before_targets = len(df_batch_final)
        df_batch_final = df_batch_final[~mask_drop].copy()
        rows_after_targets = len(df_batch_final)
        if rows_before_targets > rows_after_targets:
            print(
                f"[批处理] 已删除 {rows_before_targets - rows_after_targets} 行"
                " 目标变量全为空或全为0的记录"
            )

    # 清理内存
    del mask_all_na, mask_all_zero, mask_drop

    # 对所有"过程量列"（alias 值对应的列）的缺失值填充为 -1
    process_cols = set()
    for cfg in PROCESS_COLUMNS_CONFIG.values():
        alias = cfg.get("alias", "")
        alias_clean = alias.strip()
        if alias_clean and alias_clean in df_batch_final.columns:
            process_cols.add(alias_clean)
    process_cols = list(process_cols)
    if process_cols:
        # 分批填充缺失值，避免一次性处理太多列
        chunk_size = 50  # 每次处理50列
        for i in range(0, len(process_cols), chunk_size):
            chunk_cols = process_cols[i:i + chunk_size]
            df_batch_final[chunk_cols] = df_batch_final[chunk_cols].fillna(-1)

    # 删除 unified_time 为 NaT 的行（这些是占位记录，没有实际时间数据）
    # 在格式化时间之前就删除，更安全可靠
    rows_before = len(df_batch_final)
    df_batch_final = df_batch_final[df_batch_final["unified_time"].notna()].copy()
    rows_after = len(df_batch_final)
    if rows_before > rows_after:
        print(f"[批处理] 已删除 {rows_before - rows_after} 行 unified_time 为空的占位记录")

    # 确保时间列格式包含秒（保存为字符串格式，格式为 YYYY-MM-DD HH:MM:SS）
    # 现在所有行都有有效时间，直接格式化即可
    df_batch_final["unified_time"] = df_batch_final["unified_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df_batch_final


def main():
    df_quality, df_oper = load_base_tables()

    # 预先加载所有过程数据表（避免重复读取）
    process_data_cache: Dict[str, pd.DataFrame] = {}
    all_files = set()
    for file_list in PROCEDURE_FILES.values():
        all_files.update(file_list)

    print("预加载所有过程数据表...")
    for filename in all_files:
        if filename not in PROCESS_COLUMNS_CONFIG:
            continue
        try:
            process_data_cache[filename] = load_process_data(filename)
        except Exception as e:
            print(f"读取 {filename} 时出错：{e}，已跳过该文件。")

    # 获取所有唯一的SLAB_ID，用于分批处理
    unique_slab_ids = df_oper["SLAB_ID"].unique()
    print(f"总共有 {len(unique_slab_ids)} 个独特的SLAB_ID需要处理")
    print(f"开始分批处理，每批处理 {BATCH_SIZE} 个SLAB_ID...")

    # 数据输出路径
    out_path = os.path.join(BASE_DIR, "process_timeseries_with_quality.csv")

    # 清空输出文件
    if os.path.exists(out_path):
        os.remove(out_path)

    # 分批处理SLAB_ID，直接写入文件
    total_processed = 0
    for i in range(0, len(unique_slab_ids), BATCH_SIZE):
        batch_slab_ids = unique_slab_ids[i:i + BATCH_SIZE]
        print(f"正在处理第 {i // BATCH_SIZE + 1} 批，SLAB_ID数量: {len(batch_slab_ids)}")

        # 处理当前批次
        df_batch_result = process_batch(batch_slab_ids, df_oper, process_data_cache, df_quality)

        if not df_batch_result.empty:
            # 将结果追加到CSV文件
            header = (total_processed == 0)  # 第一批写入表头
            df_batch_result.to_csv(out_path, mode='a', header=header, index=False, encoding="utf-8-sig")
            total_processed += len(df_batch_result)
            print(f"  -> 已写入 {len(df_batch_result)} 行数据，累计 {total_processed} 行")

        # 清理当前批次数据，释放内存
        del df_batch_result

        print(f"第 {i // BATCH_SIZE + 1} 批处理完成")

    # 清理缓存
    del process_data_cache, df_quality, df_oper

    print(f"处理完成，时序结果已保存到：{out_path}")
    print(f"最终数据总行数: {total_processed}")

    # 读取最终文件获取列数和统计信息
    temp_df = pd.read_csv(out_path, nrows=0)  # 只读表头
    print(f"最终数据列数: {temp_df.shape[1]}")

    # 读取部分数据获取统计信息
    sample_df = pd.read_csv(out_path, nrows=10000)  # 读取前10000行用于统计
    print(f"\n数据统计:")
    print(f"  唯一SLAB_ID数量: {sample_df['SLAB_ID'].nunique()}")
    print(f"  唯一PROCEDURE_NAME: {sample_df['PROCEDURE_NAME'].unique()}")


if __name__ == "__main__":
    main()



