import sys
sys.path.insert(0, r'C:\Users\houce\Desktop\_streaming_platform')
from pathlib import Path
import pandas as pd
from streaming.drift_detector import DriftDetector

EVENTS = Path(r'C:\Users\houce\Desktop\ETL\bench_v6\streaming_drift\public_logs\data\public_log_events.csv')
LABELS = Path(r'C:\Users\houce\Desktop\ETL\bench_v6\streaming_drift\public_logs\data\public_log_window_labels.csv')

events_df = pd.read_csv(EVENTS)
events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], utc=True, format='mixed')
events_df = events_df.sort_values('timestamp').reset_index(drop=True)

labels_df = pd.read_csv(LABELS)
labels_df['start_time'] = pd.to_datetime(labels_df['start_time'], utc=True)
labels_df['end_time'] = pd.to_datetime(labels_df['end_time'], utc=True)

detector = DriftDetector(
    missing_threshold=0.05,
    mean_shift_sigma=4.0,
    min_std=1.0,
    high_cardinality_skip=50,
    rate_alert_multiplier=2.5,
    rate_alert_min_delta=0.05,
)

results = []
baseline_fitted = False
for _, lrow in labels_df.iterrows():
    wdf = events_df[(events_df['timestamp'] >= lrow['start_time']) & (events_df['timestamp'] < lrow['end_time'])]
    if wdf.empty:
        results.append({'gt': bool(lrow['has_anomaly']), 'pred': False, 'n': 0})
        continue
    if not baseline_fitted:
        detector.fit_baseline(wdf)
        baseline_fitted = True
        drift_events = []
    else:
        drift_events = detector.detect(wdf)
    results.append({'gt': bool(lrow['has_anomaly']), 'pred': len(drift_events) > 0, 'n': len(drift_events)})

df = pd.DataFrame(results)
tp = int((df['gt'] & df['pred']).sum())
fp = int(((~df['gt']) & df['pred']).sum())
fn = int((df['gt'] & (~df['pred'])).sum())
tn = int(((~df['gt']) & (~df['pred'])).sum())
p = tp / max(tp+fp, 1)
r = tp / max(tp+fn, 1)
f1 = 2*p*r / max(p+r, 1e-9)
acc = (tp+tn) / len(df)

print("=" * 55)
print("  DriftDetector — LogHub HDFS Full Dataset Eval")
print("=" * 55)
print(f"  Windows : {len(df)}  (anomaly={df['gt'].sum()}  clean={( ~df['gt']).sum()})")
print(f"  TP={tp}  FP={fp}  FN={fn}  TN={tn}")
print(f"  Precision : {p:.4f}")
print(f"  Recall    : {r:.4f}")
print(f"  F1        : {f1:.4f}")
print(f"  Accuracy  : {acc:.4f}")
print("=" * 55)

# Drift type breakdown
all_events = detector.summarize_events()
if not all_events.empty:
    print("\nDrift type breakdown (all windows):")
    print(all_events.groupby(['drift_type','severity'])['column'].count().to_string())
