import pandas as pd
import os
from config import RAW_OPER_TIME_FILE,OUTPUT_STATS_DIR

OPER_TIME_FILE= RAW_OPER_TIME_FILE  # 原始工序时间表路径
# ====================== 读取 & 基础处理 ======================
print("读取工序时间表...")
df = pd.read_excel(OPER_TIME_FILE)

print(f"\n总记录数: {len(df):,}")
print("列名:", list(df.columns))

# 时间列转换
df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
df['END_TIME']   = pd.to_datetime(df['END_TIME'],   errors='coerce')

# ====================== 1. 缺失与无效时间统计 ======================
print("\n" + "="*60)
print("1. 时间列缺失/无效统计")
print(df[['START_TIME', 'END_TIME']].isna().sum())
invalid_time = df['START_TIME'].isna() | df['END_TIME'].isna()
print(f"至少一个时间无效的记录数: {invalid_time.sum()} ({invalid_time.mean():.2%})")

# ====================== 2. 开始晚于结束的比例 ======================
print("\n" + "="*60)
print("2. START_TIME > END_TIME 的异常")
df['time_error'] = df['START_TIME'] > df['END_TIME']
print(df['time_error'].value_counts(normalize=True).mul(100).round(2).astype(str) + '%')
print("异常示例（前5条）：")
print(df[df['time_error']][['SLAB_ID', 'PROCEDURE_NAME', 'START_TIME', 'END_TIME']].head())

# ====================== 3. 持续时间分布（全部） ======================
df = df[~df['time_error']]  # 先暂时排除明显错误，后面可再加回来分析
df['DURATION_SEC'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds()

print("\n" + "="*60)
print("3. 所有记录持续时间分布（秒）")
print(df['DURATION_SEC'].describe(percentiles=[0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(1))

print("\n异常短 (< 30秒) 比例：", (df['DURATION_SEC'] < 30).mean().round(4)*100, "%")
print("异常长 (> 3600秒=1小时) 比例：", (df['DURATION_SEC'] > 3600).mean().round(4)*100, "%")

# ====================== 4. 按工序的持续时间统计 ======================
print("\n" + "="*60)
print("4. 按 PROCEDURE_NAME 的持续时间统计（秒）")
duration_by_proc = df.groupby('PROCEDURE_NAME')['DURATION_SEC'].describe(percentiles=[0.05, 0.5, 0.95]).round(1)
print(duration_by_proc)

# 推荐阈值建议（可手动调整）
print("\n建议的合理持续时间范围参考（可据此设置过滤规则）：")
for proc, row in duration_by_proc.iterrows():
    print(f"{proc:12}: 建议过滤 < {row['5%']*0.5:.0f}s 或 > {row['95%']*1.5:.0f}s")

# ====================== 5. 每个 SLAB_ID 的工序数量分布 ======================
print("\n" + "="*60)
print("5. 每个 SLAB_ID 包含的工序数量分布")
slab_proc_count = df.groupby('SLAB_ID')['PROCEDURE_NAME'].nunique()
print(slab_proc_count.value_counts().sort_index().to_frame('count').T)
print("\n工序数量 Top5 多 / 异常多的 SLAB_ID：")
print(slab_proc_count.nlargest(5))

print("\n工序数量很少（≤3）的 SLAB_ID 数量：", (slab_proc_count <= 3).sum())

# ====================== 6. 保存关键统计到文件 ======================
stats_summary = {
    '总记录数': len(df),
    '无效时间记录数': invalid_time.sum(),
    '开始晚于结束比例': df['time_error'].mean(),
    '持续时间中位数(秒)': df['DURATION_SEC'].median(),
    '持续时间95分位(秒)': df['DURATION_SEC'].quantile(0.95),
    '持续时间99分位(秒)': df['DURATION_SEC'].quantile(0.99),
    'SLAB_ID 总数': df['SLAB_ID'].nunique(),
    '平均每个SLAB_ID工序数': slab_proc_count.mean().round(2),
}

pd.Series(stats_summary).to_csv(os.path.join(OUTPUT_STATS_DIR, 'oper_time_summary.csv'), encoding='utf-8-sig')
duration_by_proc.to_csv(os.path.join(OUTPUT_STATS_DIR, 'duration_by_procedure.csv'), encoding='utf-8-sig')
slab_proc_count.value_counts().sort_index().to_csv(os.path.join(OUTPUT_STATS_DIR, 'slab_proc_count_dist.csv'), encoding='utf-8-sig')

print("\n统计结果已保存到:", OUTPUT_STATS_DIR)
print("建议下一步：根据上面分布，设定过滤规则（如持续时间 30s~1800s），然后输出清洗版到 filter/ 目录")