import pandas as pd
import os
import numpy as np

# ====================== 配置路径 ======================
BASE_DIR = 'E:/SGAI_Project/data_clean'
INPUT_FILE  = os.path.join(BASE_DIR, "data", "v_jk_oper_time.xlsx")          # 原始文件
FILTER_DIR  = os.path.join(BASE_DIR, "filter")
os.makedirs(FILTER_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(FILTER_DIR, "oper_time_filtered_moderate_v1.xlsx")  # 输出文件名（带版本，便于对比）

# ====================== 读取 & 基础处理 ======================
print("读取工序时间表...")
df = pd.read_excel(INPUT_FILE)

print(f"\n原始总记录数: {len(df):,}")
print("列名:", list(df.columns))

# 时间列转换
df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
df['END_TIME']   = pd.to_datetime(df['END_TIME'],   errors='coerce')

# 计算持续时间（秒）
df['DURATION_SEC'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds()

# ====================== 过滤规则（保守 + 针对短工序的合理上限） ======================
print("\n应用过滤规则...")

# 1. 删除无效时间记录
df_clean = df.dropna(subset=['START_TIME', 'END_TIME'])

# 2. 删除开始晚于结束的记录
df_clean = df_clean[df_clean['START_TIME'] <= df_clean['END_TIME']]

# 3. 排除持续时间 <= 0 的记录（主要是 *_PH 工序）
df_clean = df_clean[df_clean['DURATION_SEC'] > 0]

# 4. 排除极短记录（< 1秒，噪声或采样点）
df_clean = df_clean[df_clean['DURATION_SEC'] >= 1]

# 5. 按工序设置更合理的上限（防止几千秒的离群值）
proc_max_limits = {
    # 短工序（轧制、矫直、测厚、冷却等）
    'RM':              60,      # 原95%≈5s，放宽到1分钟
    'FM':              120,     # 原95%≈9.5s，放宽到2分钟
    'PPL':             180,     # 原95%≈34.5s
    'HPL':             180,
    'THICKNESSGAUGE':  180,
    'ACC':             300,     # 原95%≈46s，放宽到5分钟
    'UFC':             300,

    # 加热炉相关（保持较宽松）
    'FUR01_CH01_H1':   7200,
    'FUR01_CH01_H2':   7200,
    'FUR01_CH01_H3':   7200,
    'FUR01_CH01_SZ':   7200,
    'FUR01_CH02_H1':   7200,
    'FUR01_CH02_H2':   7200,
    'FUR01_CH02_H3':   7200,
    'FUR01_CH02_SZ':   7200,
    'FUR02_CH03_H1':   7200,
    'FUR02_CH03_H2':   7200,
    'FUR02_CH03_H3':   7200,
    'FUR02_CH03_SZ':   7200,
    'FUR02_CH04_H1':   7200,
    'FUR02_CH04_H2':   7200,
    'FUR02_CH04_H3':   7200,
    'FUR02_CH04_SZ':   7200,
}

# 映射上限（未定义的工序默认7200秒）
df_clean['PROC_MAX_LIMIT'] = df_clean['PROCEDURE_NAME'].map(proc_max_limits).fillna(7200)

# 应用上限过滤
before_count = len(df_clean)
df_clean = df_clean[df_clean['DURATION_SEC'] <= df_clean['PROC_MAX_LIMIT']]
dropped_by_limit = before_count - len(df_clean)

# 修正后的打印方式：先计算百分比
pct_dropped = (dropped_by_limit / before_count * 100) if before_count > 0 else 0
print(f"因超过工序上限被删除的记录数: {dropped_by_limit} ({pct_dropped:.2f}%)")

# ====================== 过滤后统计 ======================
print("\n" + "="*60)
print(f"最终过滤后记录数: {len(df_clean):,}")
print(f"总体保留比例: {len(df_clean)/len(df):.1%}")

print("\n过滤后持续时间分布（秒）：")
print(df_clean['DURATION_SEC'].describe(percentiles=[0.01, 0.05, 0.5, 0.95, 0.99]).round(1))

print("\n按 PROCEDURE_NAME 的过滤后持续时间统计（秒）：")
duration_by_proc = df_clean.groupby('PROCEDURE_NAME')['DURATION_SEC'].describe(percentiles=[0.05, 0.5, 0.95]).round(1)
print(duration_by_proc)

# ====================== 保存清洗后文件（统计完成后才 drop 辅助列） ======================
df_clean = df_clean.drop(columns=['DURATION_SEC', 'PROC_MAX_LIMIT'])
df_clean.to_excel(OUTPUT_FILE, index=False)
print(f"\n清洗后文件已保存到: {OUTPUT_FILE}")

print("\n建议：")
print("1. 使用此文件替换合表脚本中的 OPER_TIME_FILE 进行测试")
print("2. 对比 conservative 版本，看短工序的 max 是否更合理")
print("3. 如果仍有不满意的地方，可进一步收紧某些工序的上限（如 ACC/UFC 收至 180s）")