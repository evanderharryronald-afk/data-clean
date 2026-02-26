import pandas as pd
import numpy as np
from pathlib import Path
from config import RAW_OPER_TIME_FILE, FILTERED_DATA_DIR, FILTERED_REPORT_DIR

# ====================== 参数配置 ======================

OPER_TIME_FILE = RAW_OPER_TIME_FILE

# 构建输出路径（Path 版本）
OUTPUT_DIR = FILTERED_DATA_DIR / "oper_time_filtered"
OUTPUT_FILE = OUTPUT_DIR / "oper_time_filtered_auto.xlsx"
COMPARE_REPORT = FILTERED_REPORT_DIR / "oper_time_filtering_report.xlsx"

# 自动创建目录（防止 FileNotFoundError）
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FILTERED_REPORT_DIR.mkdir(parents=True, exist_ok=True)

P_UPPER = 0.95
FACTOR = 1.5
GLOBAL_MAX = 10800
GLOBAL_MIN = 1


def auto_filter_integrated():
    print("🚀 启动全功能自动化清洗与溯源对比流程...")

    # 1. 数据加载
    cols_to_use = ['SLAB_ID', 'PROCEDURE_NAME', 'START_TIME', 'END_TIME']
    df = pd.read_excel(OPER_TIME_FILE, usecols=cols_to_use)

    # 2. 时间与持续时间计算
    df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
    df['END_TIME'] = pd.to_datetime(df['END_TIME'], errors='coerce')
    df['DURATION_SEC'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds()

    # 3. 初始原因标记
    mask_missing = df['START_TIME'].isna() | df['END_TIME'].isna()
    mask_negative = (~mask_missing) & (df['DURATION_SEC'] < 0)
    mask_too_short = (~mask_missing) & (~mask_negative) & (df['DURATION_SEC'] < GLOBAL_MIN)

    # 4. 动态阈值计算
    valid_samples = df[(~mask_missing) & (~mask_negative) & (~mask_too_short)].copy()

    proc_stats = valid_samples.groupby('PROCEDURE_NAME')['DURATION_SEC'].agg(
        p95=lambda x: x.quantile(P_UPPER),
        median='median',
        mean_raw='mean',
        max_raw='max'
    ).reset_index()

    proc_stats['LIMIT_UPPER'] = (proc_stats['p95'] * FACTOR).clip(upper=GLOBAL_MAX)
    proc_stats['LIMIT_UPPER'] = np.maximum(
        proc_stats['LIMIT_UPPER'],
        proc_stats['median'] * 2
    )

    # 5. 合并阈值
    df = df.merge(
        proc_stats[['PROCEDURE_NAME', 'LIMIT_UPPER']],
        on='PROCEDURE_NAME',
        how='left'
    )

    mask_too_long = df['DURATION_SEC'] > df['LIMIT_UPPER']

    # 6. 打标签
    df['CLEAN_REASON'] = '保留'
    df.loc[mask_missing, 'CLEAN_REASON'] = '1.时间戳缺失'
    df.loc[mask_negative, 'CLEAN_REASON'] = '2.逻辑错误(负值)'
    df.loc[mask_too_short, 'CLEAN_REASON'] = '3.极短脉冲(PH类)'
    df.loc[mask_too_long, 'CLEAN_REASON'] = '4.动态超限(离群)'

    # 7. 清洗后数据
    df_filtered = df[df['CLEAN_REASON'] == '保留'].copy()

    # ====================== 报表制作 ======================

    reason_pivot = pd.crosstab(df['PROCEDURE_NAME'], df['CLEAN_REASON'])

    before_agg = (
        df[~mask_missing]
        .groupby('PROCEDURE_NAME')['DURATION_SEC']
        .agg(['count', 'max', 'mean'])
        .rename(columns={
            'count': '原始数量',
            'max': '清洗前最大值',
            'mean': '清洗前均值'
        })
    )

    after_agg = (
        df_filtered
        .groupby('PROCEDURE_NAME')['DURATION_SEC']
        .agg(['count', 'max', 'mean'])
        .rename(columns={
            'count': '清洗后数量',
            'max': '清洗后最大值',
            'mean': '清洗后均值'
        })
    )

    compare_report = pd.concat([before_agg, after_agg], axis=1)

    compare_report['保留比例'] = (
        compare_report['清洗后数量'] /
        compare_report['原始数量']
    ).fillna(0).map('{:.2%}'.format)

    summary_df = pd.DataFrame({
        '统计项': ['总原始记录', '清洗后记录', '剔除总数', '整体保留率'],
        '数值': [
            len(df),
            len(df_filtered),
            len(df) - len(df_filtered),
            f"{len(df_filtered) / len(df):.2%}"
        ]
    })

    # 8. 保存结果
    print("\n" + "=" * 20 + " 清洗前后核心指标对比 " + "=" * 20)
    print(compare_report[['原始数量', '清洗后数量', '保留比例',
                          '清洗前最大值', '清洗后最大值']].head(15))

    with pd.ExcelWriter(COMPARE_REPORT) as writer:
        summary_df.to_excel(writer, sheet_name='1.总体概览', index=False)
        compare_report.to_excel(writer, sheet_name='2.工序前后对比')
        reason_pivot.to_excel(writer, sheet_name='3.清洗原因明细')
        proc_stats.to_excel(writer, sheet_name='4.自动计算阈值参考', index=False)

    df_filtered[['SLAB_ID', 'PROCEDURE_NAME',
                 'START_TIME', 'END_TIME']].to_excel(OUTPUT_FILE, index=False)

    print("\n✅ 任务完成！")
    print(f"📊 综合对比溯源报告: {COMPARE_REPORT}")
    print(f"📦 清洗后的主数据: {OUTPUT_FILE}")


if __name__ == "__main__":
    auto_filter_integrated()