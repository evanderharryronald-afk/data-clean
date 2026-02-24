import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
import os
import gc
import matplotlib.pyplot as plt
import warnings

# 忽略 sklearn 的部分警告，以免刷屏
warnings.filterwarnings(action='ignore', category=RuntimeWarning)

# ============================
# 配置参数 (针对 6GB 显存优化)s
# ============================
CSV_FILE = r'/cleaned_data_100000.csv'
OUTPUT_FILE = r'/cleaned_data_output.csv'  # 输出文件路径
SLAB_ID_COL = 'SLAB_ID'
TIME_COL = 'unified_time'
VISUAL_DIR = r'/visuals'

# --- 关键修改：降低显存占用 ---
BATCH_SIZE = 16  # 显存不足时必须减小 (64 -> 16)
HIDDEN_DIM = 64  # 减小隐藏层维度 (128 -> 64)
NUM_LAYERS = 1  # 减少层数 (2 -> 1)
EPOCHS = 50
LEARNING_RATE = 0.001
THRESHOLD_PERCENTILE = 99

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {DEVICE}")


# ============================
# 1. 数据加载与预处理
# ============================

def load_and_preprocess(file_path):
    print(f"正在加载数据: {file_path} ...")

    # 1. 读取 CSV
    dtypes = {
        SLAB_ID_COL: 'category',
        TIME_COL: 'str'
    }
    df = pd.read_csv(file_path, dtype=dtypes, low_memory=False)
    print(f"原始数据形状: {df.shape}")

    # 2. 时间排序
    try:
        df[TIME_COL] = pd.to_datetime(df[TIME_COL])
    except KeyError:
        print(f"错误: 找不到时间列 '{TIME_COL}'，请检查 CSV 表头。")
        raise

    df = df.sort_values(by=[SLAB_ID_COL, TIME_COL])

    # 3. 特征选择
    all_cols = df.columns.tolist()
    exclude_cols = [SLAB_ID_COL, TIME_COL]
    feature_cols = [c for c in all_cols if c not in exclude_cols]

    print(f"初始选用特征数量: {len(feature_cols)}")

    # --- 关键步骤 A：强制类型转换 ---
    # errors='coerce' 会将所有非数值（包括字符串错误）变为 NaN
    df[feature_cols] = df[feature_cols].apply(pd.to_numeric, errors='coerce')

    # --- 关键步骤 B：在此刻生成独立的 Mask (基于当前是否有 NaN) ---
    # 这里的逻辑是：原始是 NaN 或无法转换的字符 -> Mask = 0 (无效)
    # 原始是有效数字 -> Mask = 1 (有效)
    # 我们使用 notna() 来判断，这比比较 -1.0 更可靠
    mask_all = df[feature_cols].notna().astype(np.float32)

    # --- 关键步骤 C：填充数据，准备后续处理 ---
    # 将所有 NaN (包括刚才转换出来的) 统一填充为 -1.0
    df[feature_cols] = df[feature_cols].fillna(-1.0)

    # --- 方差过滤 ---
    valid_feature_cols = []
    for col in feature_cols:
        # 取出不是 -1.0 的数据进行计算方差
        col_data = df[col][df[col] != -1.0]

        if len(col_data) < 2:
            continue

        variance = col_data.var()
        if variance > 0:
            valid_feature_cols.append(col)

    print(f"筛选后保留的有效特征数量: {len(valid_feature_cols)}")
    feature_cols = valid_feature_cols

    if len(feature_cols) == 0:
        raise ValueError("错误：没有可用的特征列，所有数据都被过滤了。")

    # --- 标准化 ---
    # 将 -1.0 替换为 0.0 以便 Scaler 计算 (此时 Mask 已经生成了，不用担心数据污染)
    df[feature_cols] = df[feature_cols].replace(-1.0, 0.0)

    scaler = StandardScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols]).astype(np.float32)

    # 清理标准化可能产生的异常值
    df[feature_cols] = df[feature_cols].replace([np.inf, -np.inf], 0.0)
    df[feature_cols] = df[feature_cols].fillna(0.0)

    # 4. 分组构建数据字典 (使用索引对齐 Mask)
    print("正在按 SLAB_ID 分组数据...")
    data_dict = {}

    # 同样过滤 mask_all 的列，只保留有效的特征
    mask_valid = mask_all[feature_cols]

    # --- 验证检查 ---
    print("\n=== 分离 Mask 策略数据检查 ===")
    sample_feat = feature_cols[0]
    print(f"特征 '{sample_feat}' 前3个值: {df[sample_feat].head(3).values}")
    print(f"掩码 '{sample_feat}' 前3个值: {mask_valid[sample_feat].head(3).values}")
    print("=====================\n")

    try:
        for slab_id, group in df.groupby(SLAB_ID_COL):
            # 获取这一组数据的索引
            group_index = group.index

            # 根据索引从原始 DataFrame 获取数据
            seq_data = df.loc[group_index, feature_cols].values

            # 根据索引从 mask_all (已过滤列) 获取对应的 Mask
            # 使用 .values 避免返回 DataFrame
            seq_mask = mask_valid.loc[group_index, :].values

            data_dict[slab_id] = (seq_data, seq_mask)
    except KeyError:
        print(f"错误: 找不到 ID 列 '{SLAB_ID_COL}'，请检查 CSV 表头。")
        raise

    del df, mask_all, mask_valid
    gc.collect()

    print(f"分组完成，共 {len(data_dict)} 个板坯。")
    return data_dict, feature_cols, [f"{c}_mask" for c in feature_cols], scaler


# ============================
# 2. 定义数据集类
# ============================

class SlabDataset(Dataset):
    def __init__(self, data_dict):
        self.ids = list(data_dict.keys())
        self.data_dict = data_dict

    def __len__(self):
        return len(self.ids)

    def __getitem__(self, idx):
        slab_id = self.ids[idx]
        seq_data, seq_mask = self.data_dict[slab_id]

        seq_tensor = torch.FloatTensor(seq_data)
        mask_tensor = torch.FloatTensor(seq_mask)

        return slab_id, seq_tensor, mask_tensor


def collate_fn(batch):
    batch_ids = [item[0] for item in batch]
    batch_seqs = [item[1] for item in batch]
    batch_masks = [item[2] for item in batch]

    lengths = [seq.size(0) for seq in batch_seqs]
    max_len = max(lengths)
    num_features = batch_seqs[0].size(1)

    padded_seqs = torch.zeros(len(batch), max_len, num_features)
    padded_masks = torch.zeros(len(batch), max_len, num_features)

    for i, seq in enumerate(batch_seqs):
        l = lengths[i]
        padded_seqs[i, :l, :] = seq
        padded_masks[i, :l, :] = batch_masks[i]

    return batch_ids, padded_seqs, padded_masks, torch.tensor(lengths)


# ============================
# 3. 定义 LSTM Autoencoder 模型
# ============================

class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers=2):
        super(LSTMAutoencoder, self).__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers

        self.encoder = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.decoder = nn.LSTM(hidden_dim, hidden_dim, num_layers, batch_first=True)
        self.output_layer = nn.Linear(hidden_dim, input_dim)

    def forward(self, x):
        encoder_out, (hidden, cell) = self.encoder(x)
        decoder_out, _ = self.decoder(encoder_out, (hidden, cell))
        reconstructed = self.output_layer(decoder_out)
        return reconstructed


# ============================
# 4. 训练流程 (修复旧版 PyTorch 兼容性问题)
# ============================


def train_model(model, dataloader, epochs, lr):
    criterion = nn.MSELoss(reduction='mean')
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    model.to(DEVICE)

    print(f"Start training on {DEVICE} for {epochs} epochs...")

    for epoch in range(epochs):
        model.train()
        total_loss = 0
        count = 0

        for batch_idx, (batch_ids, batch_x, batch_mask, lengths) in enumerate(dataloader):
            batch_x = batch_x.to(DEVICE)
            batch_mask = batch_mask.to(DEVICE)

            # --- 强制修复 Mask NaN ---
            if torch.isnan(batch_mask).any():
                batch_mask[torch.isnan(batch_mask)] = 0.0

            # --- 强制修复 Input NaN ---
            if torch.isnan(batch_x).any():
                batch_x[torch.isnan(batch_x)] = 0.0

            # --- 调试诊断 (仅第一个 Batch) ---
            if epoch == 0 and batch_idx == 0:
                num_valid = batch_mask.sum().item()
                total_elements = batch_mask.numel()
                print(f"\n=== 关键诊断信息 ===")
                print(f"Batch Size: {batch_x.shape}")
                print(f"Mask 中有效(1.0)的数据点数: {num_valid}")
                print(f"总数据点数: {total_elements}")
                print(f"Mask 中非0数据占比: {num_valid / total_elements:.4%}")

                if num_valid == 0:
                    print("【警告】Mask 全部为 0！有效数据被完全遮蔽。")
                    print("【尝试】强制计算忽略 Mask 的 Loss (只看模型输入输出是否相同)...")
                    temp_loss = criterion(model(batch_x), batch_x)
                    print(f"无 Mask 状态下 Loss: {temp_loss.item():.6f}")
                print(f"====================\n")

            optimizer.zero_grad()

            outputs = model(batch_x)

            diff = outputs - batch_x
            diff_sq = diff ** 2
            loss_val = diff_sq * batch_mask

            num_valid = batch_mask.sum()
            if num_valid > 0:
                loss = loss_val.sum() / num_valid
            else:
                # 如果 Mask 全为 0，我们暂时无法计算有效 Loss，设为 0 防止报错
                loss = torch.tensor(0.0, device=DEVICE, requires_grad=True)

            # 防止 loss 爆炸
            if torch.isnan(loss) or torch.isinf(loss):
                print(f"Warning: Batch {batch_idx} Loss is NaN/Inf. Skipping backward.")
                loss = torch.tensor(0.0, device=DEVICE, requires_grad=True)
            else:
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                optimizer.step()

            total_loss += loss.item()
            count += 1

        if DEVICE.type == 'cuda':
            torch.cuda.empty_cache()

        if (epoch + 1) % 10 == 0:
            print(f'Epoch [{epoch + 1}/{epochs}], Loss: {total_loss / count:.6f}')


# ============================
# 5. 推理、清洗、可视化与保存
# ============================

def detect_clean_and_save(model, data_dict, feature_cols, mask_cols, scaler, output_path, threshold_perc=99):
    model.to(DEVICE)
    model.eval()

    # 创建可视化目录
    if not os.path.exists(VISUAL_DIR):
        os.makedirs(VISUAL_DIR)
        print(f"已创建可视化目录: {VISUAL_DIR}")

    results = []
    print("正在推理、清洗并生成可视化图表...")

    MAX_VIZ_SLABS = 3
    MAX_VIZ_FEATURES = 3
    viz_count = 0

    # --- 关键修改：设置中文字体 ---
    import matplotlib.pyplot as plt
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体 (SimHei) 显示中文
    plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方框的问题

    with torch.no_grad():
        for slab_id, (seq_np, mask_np) in data_dict.items():
            seq_tensor = torch.FloatTensor(seq_np).unsqueeze(0).to(DEVICE)

            # 模型重构
            recon = model(seq_tensor)

            seq_np_curr = seq_np.copy()
            recon_np = recon.cpu().numpy().squeeze(0)
            mask_np_curr = mask_np

            error_map = np.mean((seq_np_curr - recon_np) ** 2, axis=1)
            valid_mask_per_time = mask_np_curr[:, 0]
            valid_errors = error_map[valid_mask_per_time == 1]

            if len(valid_errors) > 0:
                threshold = np.percentile(valid_errors, threshold_perc)
                if threshold < 0.01: threshold = 0.01
            else:
                threshold = 0.01

            # 检测并修正异常
            anomaly_indices = np.where((error_map > threshold) & (valid_mask_per_time == 1))[0]

            if len(anomaly_indices) > 0:
                seq_np_curr[anomaly_indices] = recon_np[anomaly_indices]

            # 反归一化
            seq_restored = scaler.inverse_transform(seq_np_curr)

            # 获取原始数据用于对比
            raw_input_normalized = data_dict[slab_id][0].copy()
            raw_original_unscaled = scaler.inverse_transform(raw_input_normalized)

            # 缺失值保留逻辑 (不强制恢复 -1.0)
            # seq_restored 已包含预测值

            # --- 可视化逻辑 ---
            if viz_count < MAX_VIZ_SLABS:
                time_indices = np.arange(len(raw_original_unscaled))

                for feat_idx in range(min(MAX_VIZ_FEATURES, len(feature_cols))):
                    feat_name = feature_cols[feat_idx]

                    valid_mask = mask_np_curr[:, feat_idx] == 1

                    if valid_mask.sum() == 0:
                        continue

                    y_original = raw_original_unscaled[valid_mask, feat_idx]
                    y_cleaned = seq_restored[valid_mask, feat_idx]
                    x_time = time_indices[valid_mask]

                    anomalies_in_feat = np.isin(anomaly_indices, np.where(valid_mask)[0])
                    anomaly_x = anomaly_indices[anomalies_in_feat]

                    temp_df = pd.DataFrame({'time_idx': x_time, 'val': y_original})
                    temp_df = temp_df[temp_df['time_idx'].isin(anomaly_x)]
                    if not temp_df.empty:
                        anomaly_x = temp_df['time_idx'].values
                        anomaly_y = temp_df['val'].values
                    else:
                        anomaly_x = []
                        anomaly_y = []

                    plt.figure(figsize=(14, 6))

                    plt.scatter(x_time, y_original, color='gray', alpha=0.5, s=20, label='原始数据')
                    plt.plot(x_time, y_cleaned, color='blue', linewidth=2, label='清洗/预测数据')

                    if len(anomaly_x) > 0:
                        plt.scatter(anomaly_x, anomaly_y, color='red', marker='x', s=100, linewidths=2, label='检测到的异常点')

                    plt.title(f"数据清洗与预测对比 - 板坯: {slab_id} | 特征: {feat_name}", fontsize=14)
                    plt.xlabel("时间步 / 采样点", fontsize=12)
                    plt.ylabel("数值", fontsize=12)
                    plt.legend(fontsize=12)
                    plt.grid(True, linestyle='--', alpha=0.6)

                    safe_filename = f"{slab_id}_{feat_name}.png"
                    save_path = os.path.join(VISUAL_DIR, safe_filename)
                    plt.savefig(save_path, dpi=150)
                    plt.close()

                viz_count += 1

            results.append({
                'SLAB_ID': slab_id,
                'cleaned_data': seq_restored
            })

    # ============================
    # 回填并保存
    # ============================
    print("正在将清洗后的数据回填并保存...")

    # 1. 构建清洗后的特征 DataFrame
    all_data_list = []
    for r in results:
        s_id = r['SLAB_ID']
        cleaned_seq = r['cleaned_data']
        T = cleaned_seq.shape[0]

        for t in range(T):
            row = {SLAB_ID_COL: s_id}
            for i, col in enumerate(feature_cols):
                row[col] = cleaned_seq[t, i]
            all_data_list.append(row)

    df_cleaned_features = pd.DataFrame(all_data_list)

    # 2. 读取原始 CSV 的所有非特征列
    print("读取原始非特征列进行合并...")
    df_other = pd.read_csv(CSV_FILE, usecols=lambda c: c not in feature_cols)
    df_other[TIME_COL] = pd.to_datetime(df_other[TIME_COL])

    # 3. 对齐并合并
    df_cleaned_features = df_cleaned_features.sort_values(by=[SLAB_ID_COL])
    df_other = df_other.sort_values(by=[SLAB_ID_COL, TIME_COL])

    if len(df_cleaned_features) != len(df_other):
        print(f"警告: 行数不匹配 (清洗后: {len(df_cleaned_features)}, 原始: {len(df_other)})。尝试使用索引合并。")
        final_df = pd.concat([df_other.reset_index(drop=True), df_cleaned_features.reset_index(drop=True)], axis=1)
    else:
        final_df = pd.concat([df_other, df_cleaned_features], axis=1)

    # 4. 保存
    final_df.to_csv(output_path, index=False)
    print(f"清洗后的数据（缺失处已保留模型预测值）已保存至: {output_path}")


# ============================
# 主程序
# ============================

if __name__ == "__main__":
    if not os.path.exists(CSV_FILE):
        print(f"错误: 找不到文件 {CSV_FILE}")
    else:
        # 1. 加载与预处理
        data_dict, feature_cols, mask_cols, scaler = load_and_preprocess(CSV_FILE)

        # 2. 创建 DataLoader
        dataset = SlabDataset(data_dict)
        dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True, collate_fn=collate_fn, num_workers=0)

        # 3. 初始化模型
        input_dim = len(feature_cols)
        model = LSTMAutoencoder(input_dim, HIDDEN_DIM, NUM_LAYERS)

        # 4. 训练
        train_model(model, dataloader, EPOCHS, LEARNING_RATE)

        # 5. 清洗与可视化保存
        detect_clean_and_save(model, data_dict, feature_cols, mask_cols, scaler, OUTPUT_FILE, THRESHOLD_PERCENTILE)

        print("所有流程执行完毕。")
