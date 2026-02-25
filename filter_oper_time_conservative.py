import pandas as pd
import os
import numpy as np

# ====================== 配置路径 ======================
BASE_DIR = 'E:/SGAI_Project/data_clean'
OPER_TIME_FILE = os.path.join(BASE_DIR, "data", "v_jk_oper_time.xlsx")  # 原始文件路径
FILTER_DIR = os.path.join(BASE_DIR, "filter")
os.makedirs(FILTER_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(FILTER_DIR, "oper_time_filtered_conservative.xlsx")  # 输出清洗后文件

# ====================== 读取 & 基础处理 ======================
print("读取工序时间表...")
df = pd.read_excel(OPER_TIME_FILE)

print(f"\n原始总记录数: {len(df):,}")
print("列名:", list(df.columns))

# 时间列转换
df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
df['END_TIME']   = pd.to_datetime(df['END_TIME'],   errors='coerce')

# 计算持续时间（秒）
df['DURATION_SEC'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds()

# ====================== 保守过滤规则 ======================
# 1. 删除无效时间记录（如果有，虽然统计中为0）
df_clean = df.dropna(subset=['START_TIME', 'END_TIME'])

# 2. 删除开始晚于结束的记录（虽然统计中为0）
df_clean = df_clean[df_clean['START_TIME'] <= df_clean['END_TIME']]

# 3. 排除持续时间 = 0 的记录（主要是 *_PH 工序）
df_clean = df_clean[df_clean['DURATION_SEC'] > 0]

# 4. 排除持续时间 < 1秒 的记录（避免纯采样点或噪声）
df_clean = df_clean[df_clean['DURATION_SEC'] >= 1]

# 5. 排除持续时间 > 7200秒（2小时）的记录（兜底上限，防止极端离群）
df_clean = df_clean[df_clean['DURATION_SEC'] <= 7200]

# ====================== 过滤后统计 ======================
print("\n" + "="*60)
print("过滤后记录数:", len(df_clean))
print(f"保留比例: {len(df_clean)/len(df):.1%}")

print("\n过滤后持续时间分布（秒）：")
print(df_clean['DURATION_SEC'].describe(percentiles=[0.01, 0.05, 0.95, 0.99]).round(1))

print("\n按 PROCEDURE_NAME 的过滤后持续时间统计（秒）：")
duration_by_proc_clean = df_clean.groupby('PROCEDURE_NAME')['DURATION_SEC'].describe(percentiles=[0.05, 0.5, 0.95]).round(1)
print(duration_by_proc_clean)

# ====================== 保存清洗后文件 ======================
df_clean = df_clean.drop(columns=['DURATION_SEC'])  # 可选：删除辅助列，保持原格式
df_clean.to_excel(OUTPUT_FILE, index=False)
print(f"\n清洗后文件已保存到: {OUTPUT_FILE}")
print("下一步建议：使用此文件替换原 OPER_TIME_FILE 进行合表测试")