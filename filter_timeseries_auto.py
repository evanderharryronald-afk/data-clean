import pandas as pd
import numpy as np
import glob
import time
import sys
from pathlib import Path
from config import RAW_TD_DIR, FILTERED_DATA_DIR, PROCEDURE_FILES, FILTERED_REPORT_DIR

# ====================== 配置 ======================

CLEANED_TD_DIR = FILTERED_DATA_DIR / "tb_filtered"
REPORT_PATH = FILTERED_REPORT_DIR / "tb_detailed_filtering_report.xlsx"

# 自动创建目录
CLEANED_TD_DIR.mkdir(parents=True, exist_ok=True)
FILTERED_REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ====================== 策略识别 ======================

def get_strategy_from_filename(filename: str) -> str:
    fn = filename.lower()
    if "speed" in fn: return "SPEED"
    if "temp" in fn or "pyro" in fn: return "TEMP"
    if "force" in fn or "load" in fn: return "FORCE"
    if "press" in fn or "water" in fn: return "PRESS"
    if "thick" in fn: return "THICK"
    return "DEFAULT"


# ====================== 单文件清洗 ======================

def clean_ts_file_dynamic(filepath: Path):
    filename = filepath.name
    strategy = get_strategy_from_filename(filename)

    df = pd.read_csv(filepath)
    raw_count = len(df)

    if raw_count == 0 or 'val' not in df.columns:
        return None

    val = df['val']

    # ================= 动态分布统计 =================
    q_min, q_max = val.quantile(0.001), val.quantile(0.999)
    p05, p95 = val.quantile(0.05), val.quantile(0.95)

    auto_upper = p95 * 2.0 if p95 > 0 else p95 * 0.5
    auto_lower = p05 * 2.0 if p05 < 0 else p05 * 0.5

    if strategy == "TEMP":
        auto_lower = max(auto_lower, 0)

    # --- 1. Quality过滤 ---
    if 'quality' in df.columns:
        mask_quality = df['quality'].isin([1, 2])
    else:
        mask_quality = pd.Series(False, index=df.index)

    # --- 2. 动态红线 ---
    mask_physical = (val < auto_lower) | (val > auto_upper)

    # --- 3. 统计毛刺 ---
    std_val = val.std()

    if std_val > 0:
        rolling_median = val.rolling(5, center=True).median()
        mask_spike = (val - rolling_median).abs() > (std_val * 8)
    else:
        mask_spike = pd.Series(False, index=df.index)

    # ================= 打标签 =================
    df['REASON'] = '保留'
    df.loc[mask_spike, 'REASON'] = '3.瞬间毛刺'
    df.loc[mask_physical, 'REASON'] = '2.物理红线'
    df.loc[mask_quality, 'REASON'] = '1.Quality异常'

    # ================= 清洗后数据 =================
    df_clean = df[df['REASON'] == '保留'].copy()

    if strategy == "THICK":
        df_clean.loc[df_clean['val'] < 0, 'val'] = 0

    # ================= 统计信息 =================
    res_counts = df['REASON'].value_counts()

    stats = {
        'filename': filename,
        'type': strategy,
        'raw_count': raw_count,
        'clean_count': len(df_clean),
        '1.Quality异常': res_counts.get('1.Quality异常', 0),
        '2.物理红线': res_counts.get('2.物理红线', 0),
        '3.瞬间毛刺': res_counts.get('3.瞬间毛刺', 0),
        'min_raw': val.min(),
        'max_raw': val.max(),
        'mean_raw': val.mean(),
        'min_clean': df_clean['val'].min() if len(df_clean) > 0 else np.nan,
        'max_clean': df_clean['val'].max() if len(df_clean) > 0 else np.nan,
        'dynamic_lower': auto_lower,
        'dynamic_upper': auto_upper
    }

    # 保存清洗文件
    output_path = CLEANED_TD_DIR / filename
    df_clean.drop(columns=['REASON']).to_csv(output_path, index=False)

    return stats


# ====================== 主流程 ======================

def main():
    start_time = time.time()
    print("🚀 启动[动态阈值]时序清理程序...")

    all_files_path = []

    for procedure, patterns in PROCEDURE_FILES.items():
        for pattern in patterns:
            all_files_path.extend(RAW_TD_DIR.glob(pattern))

    target_files = sorted(set(all_files_path))
    results = []

    for i, filepath in enumerate(target_files):
        sys.stdout.write(
            f"\r[{i + 1}/{len(target_files)}] 正在动态分析并清洗: {filepath.name[:30]}..."
        )
        sys.stdout.flush()

        res = clean_ts_file_dynamic(filepath)
        if res:
            results.append(res)

    print("\n\n📊 生成报告中...")

    report_df = pd.DataFrame(results)

    if len(report_df) > 0:
        report_df['保留率'] = (
            report_df['clean_count'] / report_df['raw_count']
        ).map('{:.2%}'.format)

        COLUMN_ORDER = [
            'filename', 'type', 'raw_count', 'clean_count', '保留率',
            '1.Quality异常', '2.物理红线', '3.瞬间毛刺',
            'dynamic_lower', 'dynamic_upper',
            'min_raw', 'max_raw', 'min_clean', 'max_clean'
        ]

        report_df[COLUMN_ORDER].to_excel(REPORT_PATH, index=False)

    duration = time.time() - start_time
    print(f"\n✅ 完成！总耗时: {duration:.2f}s")
    print(f"📄 报告位置: {REPORT_PATH}")


if __name__ == "__main__":
    main()