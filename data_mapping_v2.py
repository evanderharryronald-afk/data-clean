import os
import glob
import time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from config import OUTPUT_DIR, RAW_TD_DIR, RAW_OPER_TIME_FILE, RAW_QUALITY_FILE, PROCEDURE_FILES, PROCESS_COLUMNS_CONFIG
from config import FILTERED_TD_DIR, FILTERED_OPER_TIME_FILE, FILTERED_QUALITY_FILE

# Configuration Paths
TD_DIR = RAW_TD_DIR
OPER_TIME_FILE = RAW_OPER_TIME_FILE
QUALITY_FILE = RAW_QUALITY_FILE
OUT_PATH = OUTPUT_DIR / "process_timeseries_imputed_v4_trend.csv"


def log_event(level, msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{level} {timestamp}] {msg}")


ALL_ALIASES = [cfg["alias"] for cfg in PROCESS_COLUMNS_CONFIG.values()]
TIME_PARSE_KWARGS = {"errors": "coerce"}
BATCH_SIZE = 500


# ====================== 1. StatsFactory: Deep Feature Extraction ======================
class StatsFactory:
    def __init__(self, raw_dir, config):
        self.raw_dir = raw_dir
        self.config = config

    def analyze(self) -> Dict:
        log_event("STEP", "Phase 0: Modeling physical DNA of sensors (Single-file mode)...")
        stats_lookup = {}
        for file_prefix, cfg in self.config.items():
            alias = cfg['alias']
            files = glob.glob(os.path.join(self.raw_dir, f"{file_prefix}*.csv"))
            if not files: continue

            try:
                # Load full file as there is only one per feature
                df = pd.read_csv(files[0], usecols=['val'])
                combined = df['val'].dropna()
            except:
                continue

            if combined.empty: continue

            force_zero_keywords = ['SPEED', 'FORCE', 'PRESS', 'RED', 'THICK']
            is_mechanical = any(k in alias for k in force_zero_keywords)

            if is_mechanical:
                ambient = 0.0
                # Use absolute values to avoid zero-mean cancellation in RM_SPEED
                abs_vals = combined.abs()
                # Focus on the 75th percentile to capture the "Actual Rolling" state
                work_threshold = abs_vals.quantile(0.75)
                work_data = abs_vals[abs_vals >= work_threshold]

                if not work_data.empty:
                    work_mean = work_data.median()  # Robust to outliers
                    # Ensure at least 5% variance to prevent "Constant Line" filling
                    work_std = max(work_data.std(), work_mean * 0.05)
                else:
                    work_mean = abs_vals.max() * 0.7
                    work_std = work_mean * 0.1
            else:
                # Temperature: P05 as ambient floor
                ambient = max(combined.quantile(0.05), 20.0)
                work_mean = combined.median()
                work_std = combined.std()

            stats_lookup[alias] = {
                "ambient": ambient,
                "work_mean": work_mean,
                "work_std": work_std,
                "is_mechanical": is_mechanical
            }
        log_event("INFO", "Physical DNA modeling complete.")
        return stats_lookup


# ====================== 2. Physical Imputation with Trend Smoothing ======================
def impute_with_physics(df_batch: pd.DataFrame, stats_lookup: Dict):
    mask_dict = {}
    stats_a, stats_b = 0, 0

    for col in ALL_ALIASES:
        if col not in df_batch.columns: continue

        # Identify actual observations (filter out sensor drift near zero)
        is_real = (df_batch[col].abs() > 1e-2) & (df_batch[col].notnull())
        mask_dict[f"{col}_MSK"] = is_real.astype('uint8')

        stat = stats_lookup.get(col)
        if not stat: continue

        proc_prefix = col.split('_')[0]
        in_work_zone = df_batch['PROCEDURE_NAME'].str.contains(proc_prefix, na=False)

        # Type A: Missing within Procedure (Gap Filling)
        mask_a = (~is_real) & in_work_zone
        num_missing = mask_a.sum()
        if num_missing > 0:
            # Generate random noise based on physical state
            # Force vibration for mechanical parts to avoid constant values
            effective_std = max(stat['work_std'], stat['work_mean'] * 0.02)
            noise = np.random.normal(stat['work_mean'], effective_std * 0.6, size=num_missing)

            # Trend Smoothing for Temperature (Simulate Thermal Inertia)
            if "PYRO" in col or "TEMP" in col:
                # Rolling mean transforms white noise into physical trends
                noise = pd.Series(noise).rolling(window=min(25, num_missing), min_periods=1, center=True).mean().values

            # Index-locked assignment to prevent broadcasting constants
            df_batch.loc[mask_a, col] = noise
            stats_a += num_missing

        # Type B: Structural Missing (Background Noise)
        mask_b = (~is_real) & (~in_work_zone)
        if mask_b.any():
            df_batch.loc[mask_b, col] = stat['ambient']
            stats_b += mask_b.sum()

    if mask_dict:
        df_masks = pd.DataFrame(mask_dict, index=df_batch.index)
        df_batch = pd.concat([df_batch, df_masks], axis=1)

    return df_batch, stats_a, stats_b


# ====================== 3. Utility Functions ======================
def get_file_config(filename: str) -> Optional[Dict]:
    base_name = os.path.splitext(filename)[0]
    parts = base_name.split('_')
    for i in range(len(parts), 0, -1):
        prefix = '_'.join(parts[:i])
        if prefix in PROCESS_COLUMNS_CONFIG: return PROCESS_COLUMNS_CONFIG[prefix]
    return None


def discover_csv_files():
    discovered = {}
    for proc, patterns in PROCEDURE_FILES.items():
        actual = []
        for p in patterns:
            matching = glob.glob(os.path.join(TD_DIR, p))
            actual.extend([os.path.basename(f) for f in matching if f.endswith('.csv')])
        discovered[proc] = sorted(list(set(actual)))
    return discovered


def load_base_tables():
    log_event("STEP", "Loading Excel configuration tables...")
    df_q = pd.read_excel(QUALITY_FILE)
    df_o = pd.read_excel(OPER_TIME_FILE)
    if "SLAB_ID" not in df_q.columns:
        df_q = df_q.rename(columns={"FUR_EXIT_SLAB_ID": "SLAB_ID"})
    df_o["START_TIME"] = pd.to_datetime(df_o["START_TIME"], **TIME_PARSE_KWARGS)
    df_o["END_TIME"] = pd.to_datetime(df_o["END_TIME"], **TIME_PARSE_KWARGS)
    return df_q, df_o


def load_process_data(filename):
    cfg = get_file_config(filename)
    df = pd.read_csv(os.path.join(TD_DIR, filename))
    t_col = cfg["time_col"]
    df[t_col] = pd.to_datetime(df[t_col], **TIME_PARSE_KWARGS).dt.floor("1s")
    return df.dropna(subset=[t_col]).sort_values(t_col)


def extract_timesteps_by_time_window(df_proc, df_oper_proc, file_cfg, procedure, alias):
    t_col = file_cfg["time_col"]
    val_col = next(v for v in file_cfg["value_cols"] if v in df_proc.columns)
    times, starts, ends, sids = df_proc[t_col].values, df_oper_proc["START_TIME"].values, df_oper_proc[
        "END_TIME"].values, df_oper_proc["SLAB_ID"].values
    res = []
    for s, e, sid in zip(starts, ends, sids):
        l, r = np.searchsorted(times, [s, e])
        if l >= r: continue
        win = df_proc.iloc[l:r].copy()
        win = win.rename(columns={t_col: "unified_time", val_col: alias})
        win["SLAB_ID"], win["PROCEDURE_NAME"] = sid, procedure
        res.append(win[["SLAB_ID", "PROCEDURE_NAME", "unified_time", alias]])
    return pd.concat(res) if res else pd.DataFrame()


def align_and_merge_procedure_data(dfs, procedure, slab_id):
    if not dfs: return pd.DataFrame()
    m = pd.concat(dfs, ignore_index=True)
    m["unified_time"] = m["unified_time"].dt.floor("1s")
    aggs = {c: "mean" for c in m.columns if c not in ["SLAB_ID", "PROCEDURE_NAME", "unified_time"]}
    return m.groupby(["SLAB_ID", "PROCEDURE_NAME", "unified_time"], as_index=False).agg(aggs)


# ====================== 4. Core Pipeline ======================
def process_batch(slab_ids, df_oper, cache, df_quality, discovered, stats_lookup):
    batch_list = []
    for sid in slab_ids:
        df_o_slab = df_oper[df_oper["SLAB_ID"] == sid]
        seqs = []
        for proc in df_o_slab["PROCEDURE_NAME"].unique():
            df_o_p = df_o_slab[df_o_slab["PROCEDURE_NAME"] == proc]
            dfs = []
            for fn in discovered.get(proc, []):
                if fn not in cache: continue
                cfg = get_file_config(fn)
                df_ts = extract_timesteps_by_time_window(cache[fn], df_o_p, cfg, proc, cfg["alias"])
                if not df_ts.empty: dfs.append(df_ts)
            if dfs: seqs.append(align_and_merge_procedure_data(dfs, proc, sid))
        if seqs: batch_list.append(pd.concat(seqs, ignore_index=True))

    if not batch_list: return pd.DataFrame(), 0, 0

    df_b = pd.concat(batch_list, ignore_index=True)
    df_b = df_b.merge(df_quality, on="SLAB_ID", how="left")

    for col in ALL_ALIASES:
        if col not in df_b.columns:
            df_b[col] = 0.0
        else:
            df_b[col] = df_b[col].fillna(0.0)

    df_b, cnt_a, cnt_b = impute_with_physics(df_b, stats_lookup)

    fixed = ["SLAB_ID", "unified_time", "PROCEDURE_NAME"]
    msk_cols = [c for c in df_b.columns if c.endswith("_MSK")]
    other_cols = [c for c in df_b.columns if c not in (fixed + ALL_ALIASES + msk_cols)]
    return df_b[fixed + ALL_ALIASES + msk_cols + other_cols], cnt_a, cnt_b


def main():
    total_start_time = time.time()
    log_event("START", "==== Launching Imputation Pipeline v4 (Trend-Aware) ====")

    df_quality, df_oper = load_base_tables()
    all_slab_ids = df_oper["SLAB_ID"].unique()
    total_slabs = len(all_slab_ids)
    log_event("INFO", f"Total Slab_ID to process: {total_slabs}")

    disco = discover_csv_files()
    stats_lookup = StatsFactory(TD_DIR, PROCESS_COLUMNS_CONFIG).analyze()

    cache = {}
    all_f = set().union(*disco.values())
    log_event("STEP", f"Pre-loading {len(all_f)} process files...")
    for f in all_f:
        try:
            cache[f] = load_process_data(f)
        except:
            continue

    if os.path.exists(OUT_PATH): os.remove(OUT_PATH)

    first_write = True
    total_rows = 0
    num_batches = (total_slabs + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, total_slabs, BATCH_SIZE):
        batch = all_slab_ids[i:i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        log_event("PROCESS", f"Batch [{batch_idx}/{num_batches}] | Size: {len(batch)}")

        df_batch, ca, cb = process_batch(batch, df_oper, cache, df_quality, disco, stats_lookup)

        if not df_batch.empty:
            df_batch.to_csv(OUT_PATH, mode='a', header=first_write, index=False, encoding="utf-8-sig")
            first_write = False
            total_rows += len(df_batch)
            log_event("DETAIL", f"Batch saved: +{len(df_batch)} rows | Fill-A: {ca} pts | Fill-B: {cb} pts")

    log_event("SUCCESS", "==== Pipeline Complete ====")
    log_event("SUMMARY", f"Final Output: {OUT_PATH}")
    log_event("SUMMARY", f"Total Rows: {total_rows:,}")
    log_event("SUMMARY", f"Execution Time: {time.time() - total_start_time:.2f}s")


if __name__ == "__main__":
    main()