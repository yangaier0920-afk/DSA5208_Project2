"""Record the user's accepted recommendation bundle while E remains unresolved."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
pending_path = ROOT / "configs" / "project_decisions.pending.json"
data = json.loads(pending_path.read_text(encoding="utf-8"))
for key, item in data["items"].items():
    if key != "E":
        selected = item["recommended"]
        if item["selected"] is not None and item["selected"] != selected:
            raise RuntimeError(f"Do not overwrite an existing different decision: {key}")
        item["selected"] = selected
        item["decision_source"] = "Direct user acceptance of recommended bundle, except unresolved E"
e = data["items"]["E"]
if e["selected"] is not None:
    raise RuntimeError("E already selected; this script must not erase that decision")
e["recommended"] = "E2"
e["user_preference"] = "Interested in E2; uncertain about results; not yet confirmed"
e["proposal_document"] = "docs/E2滚动验证实验方案.md"
data.update({"status": "PARTIALLY_CONFIRMED", "applied": False,
             "confirmed_at_sgt": datetime.now(timezone(timedelta(hours=8))).isoformat(),
             "confirmed_items": [key for key in data["items"] if key != "E"], "pending_items": ["E"],
             "user_statement": "采用推荐组合，但 E 我有点纠结，觉得E2的方式作为实验更加有趣和有意义，但不知道最终结果如何。"})
pending_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
selected = {key: item["selected"] for key, item in data["items"].items()}
configuration = {"status": "CLEANING_RULES_CONFIRMED_E_PENDING", "applied": False,
                 "decision_record": pending_path.relative_to(ROOT).as_posix(), "selected": selected,
                 "data_version": "v1", "timezone": "Asia/Singapore",
                 "observations": {"preserve_raw": True, "storage": "sparse_parquet", "partition_by": ["year", "month"],
                     "event_ts": "original parsed observation timestamp",
                     "slot_end_ts": "event_ts + 1s only when epoch_s mod 300 == 299; otherwise unchanged",
                     "primary_key": ["station_id", "slot_end_ts"], "rainfall_interval": "ending at slot_end_ts",
                     "rain_period_start_ts": "slot_end_ts - 300s", "task1_period_grouping": "rain_period_start_ts in SGT",
                     "aligned_collision_rule": "equal numeric rainfall: keep the original exact-grid row, preserve both lineages and metadata history",
                     "unequal_rainfall_collision_rule": "quarantine whole group",
                     "missing_rainfall": "unknown; never zero-fill", "retain_zero": True, "retain_finite_nonnegative_extremes": True,
                     "label_decimal_scale": "lossless scale established from source precision; fail rather than silently round",
                     "station_id_policy": "preserve original ID including S113; preserve row metadata/history",
                     "expected_rows_only_if_no_additional_exclusions": 48538627},
                 "windows": {"future_offsets_seconds": [300,600,900,1200,1500,1800],
                             "require_all_future_slots": True, "historical_offsets_seconds": list(range(-3300,1,300)),
                             "require_all_12_historical_slots": True, "label_rule": "future_30m_mm > threshold_mm",
                             "information_availability": "historical retrospective event-time assumption"},
                 "task1": {"common_station_candidates": 38, "calendar_month_min_coverage": 0.90,
                           "sensitivity_min_coverage": 0.95, "comparison": "same-month station intersection among years compared",
                           "recompute_panels_after_cleaning": True, "filter_global_observations": False, "filter_task2_by_month_coverage": False},
                 "training": {"time_protocol": None, "proposal": "E2; awaiting confirmation",
                             "pipeline_sample_fraction": 0.01, "formal_anchor_sample_fraction": 0.10,
                             "threshold_candidate_values_mm": [0,0.5,1,2,5], "threshold_as_numeric_feature": True,
                             "inference_policy": "strict supported station/time/history/threshold scope"}}
(ROOT / "configs" / "project.json").write_text(json.dumps(configuration, ensure_ascii=False, indent=2), encoding="utf-8")
assert len(data["confirmed_items"]) == 14 and selected["E"] is None
print("Recorded 14 confirmed items; E remains pending. No cleaning or training applied.")
