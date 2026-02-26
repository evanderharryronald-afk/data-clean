from pathlib import Path
from typing import Dict, List

"""
时序数据合表 - 修复版（含运行时间统计）
工程级路径管理版本（跨平台 + 自动目录创建）
"""

# ====================== 基础路径 ======================

BASE_DIR = Path("E:/SGAI_Project/data_clean").resolve()

RAW_DATA_DIR = BASE_DIR / "data"
FILTERED_DATA_DIR = BASE_DIR / "filter"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_STATS_DIR = BASE_DIR / "stats"

# ====================== RAW DATA ======================

RAW_TD_DIR = RAW_DATA_DIR / "tb"
RAW_QUALITY_FILE = RAW_DATA_DIR / "select_v_quality_all.xlsx"
RAW_OPER_TIME_FILE = RAW_DATA_DIR / "v_jk_oper_time.xlsx"

# ====================== FILTERED DATA ======================

FILTERED_REPORT_DIR = FILTERED_DATA_DIR / "filter_reports"
FILTERED_TD_DIR = FILTERED_DATA_DIR / "tb_filtered"

# 质量数据暂时直接使用原始版本
FILTERED_QUALITY_FILE = RAW_QUALITY_FILE

# 工序时间数据（可切换版本）
FILTERED_OPER_TIME_FILE = FILTERED_DATA_DIR / "oper_time_filtered_moderate_v1.xlsx"

# ====================== 自动创建关键目录 ======================

for path in [
    RAW_DATA_DIR,
    FILTERED_DATA_DIR,
    OUTPUT_DIR,
    OUTPUT_STATS_DIR,
    FILTERED_REPORT_DIR,
    FILTERED_TD_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)

# ====================== 工序文件映射 ======================

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
