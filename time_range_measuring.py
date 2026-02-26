# import pandas as pd
# import numpy as np
# import time
# import sys
# from pathlib import Path
# from config import RAW_TD_DIR,STATS_DIR
#
# # ===================== 配置 =====================
# DATA_DIR = RAW_TD_DIR   # 修改为你的路径
# REPORT_PATH = STATS_DIR/"time_range_gap_summary.xlsx"
# GAP_THRESHOLD_MULTIPLIER = 5
#
#
# # ====================== 单文件统计 ======================
#
# def analyze_time_file(filepath: Path):
#     filename = filepath.name
#
#     df = pd.read_csv(filepath)
#
#     if 'ts' not in df.columns or len(df) < 2:
#         return None
#
#     # 时间转换
#     df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
#     df = df.dropna(subset=['ts'])
#
#     if len(df) < 2:
#         return None
#
#     df = df.sort_values('ts').reset_index(drop=True)
#
#     # 计算时间差
#     df['time_diff'] = df['ts'].diff()
#
#     start_time = df['ts'].iloc[0]
#     end_time = df['ts'].iloc[-1]
#     duration = end_time - start_time
#     row_count = len(df)
#
#     min_gap = df['time_diff'].min()
#     median_gap = df['time_diff'].median()
#     max_gap = df['time_diff'].max()
#
#     expected_interval = median_gap
#     gap_threshold = expected_interval * GAP_THRESHOLD_MULTIPLIER
#
#     abnormal_gaps = df[df['time_diff'] > gap_threshold]
#
#     return {
#         'filename': filename,
#         'start_time': start_time,
#         'end_time': end_time,
#         'duration': duration,
#         'row_count': row_count,
#         'min_gap': min_gap,
#         'median_gap': median_gap,
#         'max_gap': max_gap,
#         'expected_interval': expected_interval,
#         'gap_threshold': gap_threshold,
#         'abnormal_gap_count': len(abnormal_gaps),
#         'has_abnormal_gap': len(abnormal_gaps) > 0
#     }
#
#
# # ====================== 主流程 ======================
#
# def main():
#     start_time = time.time()
#     print("🚀 启动时序时间范围 & 断层分析程序...")
#
#     target_files = sorted(RAW_TD_DIR.glob("*.csv"))
#     results = []
#
#     total_files = len(target_files)
#
#     for i, filepath in enumerate(target_files):
#
#         sys.stdout.write(
#             f"\r[{i + 1}/{total_files}] 正在分析时间结构: {filepath.name[:40]}..."
#         )
#         sys.stdout.flush()
#
#         res = analyze_time_file(filepath)
#         if res:
#             results.append(res)
#
#     print("\n\n📊 生成时间结构报告中...")
#
#     report_df = pd.DataFrame(results)
#
#     if len(report_df) > 0:
#         report_df = report_df.sort_values('start_time')
#         report_df.to_excel(REPORT_PATH, index=False)
#
#     duration = time.time() - start_time
#     print(f"\n✅ 完成！总耗时: {duration:.2f}s")
#     print(f"📄 报告位置: {REPORT_PATH}")
#
#
# if __name__ == "__main__":
#     main()

import pandas as pd
import numpy as np
import time
import sys
from pathlib import Path
from config import RAW_TD_DIR, STATS_DIR

# ===================== 配置 =====================
DATA_DIR = RAW_TD_DIR
REPORT_PATH = STATS_DIR / "td_data_quality_profile.xlsx"
STATS_DIR.mkdir(parents=True, exist_ok=True)

GAP_THRESHOLD_MULTIPLIER = 5


# ====================== 单文件分析 ======================

def analyze_time_file(filepath: Path):
    filename = filepath.name

    df = pd.read_csv(filepath)

    if 'ts' not in df.columns or len(df) < 2:
        return None

    # ---------------- 时间处理 ----------------
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    df = df.dropna(subset=['ts'])
    df = df.sort_values('ts').reset_index(drop=True)

    if len(df) < 2:
        return None

    df['time_diff'] = df['ts'].diff()

    start_time = df['ts'].iloc[0]
    end_time = df['ts'].iloc[-1]
    duration = end_time - start_time
    row_count = len(df)

    min_gap = df['time_diff'].min()
    median_gap = df['time_diff'].median()
    max_gap = df['time_diff'].max()

    expected_interval = median_gap
    gap_threshold = expected_interval * GAP_THRESHOLD_MULTIPLIER
    abnormal_gap_count = (df['time_diff'] > gap_threshold).sum()

    duplicate_ts_count = df['ts'].duplicated().sum()

    # ---------------- 数值分布 ----------------
    val = df['val']

    mean_val = val.mean()
    std_val = val.std()
    min_val = val.min()
    max_val = val.max()

    p01 = val.quantile(0.01)
    p99 = val.quantile(0.99)

    skewness = val.skew()

    zero_ratio = (val == 0).mean()

    # 相邻相同值比例（检测卡死）
    constant_ratio = (val.diff() == 0).mean()

    # 3σ异常比例
    if std_val > 0:
        outlier_ratio = ((val - mean_val).abs() > 3 * std_val).mean()
    else:
        outlier_ratio = 0

    # ---------------- quality统计 ----------------
    if 'quality' in df.columns:
        quality_bad_ratio = (~df['quality'].isin([0])).mean()
    else:
        quality_bad_ratio = np.nan

    # ---------------- tow/toc合理性 ----------------
    if 'tow' in df.columns and 'toc' in df.columns:
        df['tow'] = pd.to_datetime(df['tow'], errors='coerce')
        df['toc'] = pd.to_datetime(df['toc'], errors='coerce')

        invalid_tow_toc = (df['tow'] > df['toc']).sum()
        ts_outside_window = ((df['ts'] < df['tow']) | (df['ts'] > df['toc'])).sum()
    else:
        invalid_tow_toc = np.nan
        ts_outside_window = np.nan

    return {
        # 基本信息
        'filename': filename,
        'row_count': row_count,
        'start_time': start_time,
        'end_time': end_time,
        'duration': duration,

        # 时间结构
        'median_gap': median_gap,
        'max_gap': max_gap,
        'abnormal_gap_count': abnormal_gap_count,
        'duplicate_ts_count': duplicate_ts_count,

        # 数值分布
        'min_val': min_val,
        'max_val': max_val,
        'mean_val': mean_val,
        'std_val': std_val,
        'p01': p01,
        'p99': p99,
        'skewness': skewness,
        'zero_ratio': zero_ratio,
        'constant_ratio': constant_ratio,
        'outlier_ratio': outlier_ratio,

        # 质量列
        'quality_bad_ratio': quality_bad_ratio,

        # 工艺窗口合理性
        'invalid_tow_toc': invalid_tow_toc,
        'ts_outside_window': ts_outside_window
    }


# ====================== 主流程 ======================

def main():
    start_time = time.time()
    print("🚀 启动工业时序数据质量画像分析程序...")

    target_files = sorted(DATA_DIR.glob("*.csv"))
    results = []

    total_files = len(target_files)

    for i, filepath in enumerate(target_files):
        sys.stdout.write(
            f"\r[{i + 1}/{total_files}] 正在分析数据质量: {filepath.name[:40]}..."
        )
        sys.stdout.flush()

        res = analyze_time_file(filepath)
        if res:
            results.append(res)

    print("\n\n📊 生成数据质量画像报告中...")

    report_df = pd.DataFrame(results)

    if len(report_df) > 0:
        report_df = report_df.sort_values('start_time')
        report_df.to_excel(REPORT_PATH, index=False)

    duration = time.time() - start_time
    print(f"\n✅ 完成！总耗时: {duration:.2f}s")
    print(f"📄 报告位置: {REPORT_PATH}")


if __name__ == "__main__":
    main()