import pandas as pd
from collections import Counter
import os

# ================== 配置 ==================
OUTPUT_PATH = r'E:/SGAI_Project/data_clean/output/process_timeseries_clean.csv'
CHUNK_SIZE = 1_000_000  # 根据你内存调整，越大越快
# =========================================

if not os.path.exists(OUTPUT_PATH):
    print("❌ 输出文件不存在！请先生成文件")
    exit()

print("正在快速扫描输出文件...（只读两列，分块处理）")

dupe_counter = Counter()  # key: (SLAB_ID, unified_time) → 出现次数
total_rows = 0
max_dupe = 0
dupe_groups = 0

for i, chunk in enumerate(pd.read_csv(OUTPUT_PATH,
                                      usecols=['SLAB_ID', 'unified_time'],
                                      chunksize=CHUNK_SIZE,
                                      dtype={'SLAB_ID': 'str'},  # 防止SLAB_ID类型不一致
                                      low_memory=False)):
    chunk['key'] = chunk['SLAB_ID'].astype(str) + '|' + chunk['unified_time']
    counts = chunk['key'].value_counts()

    for key, cnt in counts.items():
        dupe_counter[key] += cnt
        if cnt > 1:
            dupe_groups += 1
            max_dupe = max(max_dupe, cnt)

    total_rows += len(chunk)
    print(f"  已扫描 {total_rows:,} 行...")

# 统计结果
total_dupe_occurrences = sum(1 for v in dupe_counter.values() if v > 1)  # 有重复的组数
total_duplicate_rows = sum(v - 1 for v in dupe_counter.values() if v > 1)  # 多出来的行数

print("\n" + "=" * 60)
print("检查完成！")
print(f"总行数          : {total_rows:,}")
print(f"唯一时间点组数   : {len(dupe_counter):,}")
print(f"有重复的时间点组数 : {total_dupe_occurrences:,}  ({total_dupe_occurrences / len(dupe_counter) * 100:.2f}%)")
print(f"最多重复的秒数   : {max_dupe} 行")
print(f"总多余重复行数   : {total_duplicate_rows:,} 行")
print("=" * 60)

if total_dupe_occurrences > 0:
    print("\n前10个最严重的重复示例（SLAB_ID | 时间 | 重复次数）：")
    sorted_dupes = sorted(
        [(k.split('|')[0], k.split('|')[1], v) for k, v in dupe_counter.items() if v > 1],
        key=lambda x: x[2], reverse=True
    )[:10]
    for slab, t, cnt in sorted_dupes:
        print(f"  {slab} | {t} → {cnt} 行")
else:
    print("🎉 完美！没有同一秒重复行！")

# 可选：保存详细重复统计
if total_dupe_occurrences > 0:
    detail = pd.DataFrame([
        {'SLAB_ID': k.split('|')[0], 'unified_time': k.split('|')[1], 'count': v}
        for k, v in dupe_counter.items() if v > 1
    ])
    detail.to_csv(os.path.join(os.path.dirname(OUTPUT_PATH), 'duplicate_report.csv'), index=False, encoding='utf-8-sig')
    print(f"\n详细重复报告已保存到：duplicate_report.csv")