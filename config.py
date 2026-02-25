import os

"""
时序数据合表 - 修复版（含运行时间统计）
"""

BASE_DIR = 'E:/SGAI_Project/data_clean'
RAW_DATA_DIR = os.path.join(BASE_DIR, "data")
FILTERED_DATA_DIR = os.path.join(BASE_DIR, "filter")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# RAW_DATA
RAW_TD_DIR = os.path.join(RAW_DATA_DIR, "tb")
RAW_QUALITY_FILE = os.path.join(RAW_DATA_DIR, "select_v_quality_all.xlsx")
RAW_OPER_TIME_FILE = os.path.join(RAW_DATA_DIR, "v_jk_oper_time.xlsx")

# FILTERED_DATA
# 质量数据暂时不做过滤，直接使用原始文件（后续如果需要过滤，可以在这里修改路径）
FILTERED_QUALITY_FILE = os.path.join(RAW_DATA_DIR, "select_v_quality_all.xlsx")

# 工序时间数据使用过滤后的版本（目前是 moderate 版本，后续如果需要切换到 conservative 版本，可以在这里修改路径）
FILTERED_OPER_TIME_FILE = os.path.join(FILTERED_DATA_DIR, "oper_time_filtered_moderate_v1.xlsx")

# 过滤后的时序数据目录
FILTERED_TD_DIR = os.path.join(FILTERED_DATA_DIR, "tb_filtered")