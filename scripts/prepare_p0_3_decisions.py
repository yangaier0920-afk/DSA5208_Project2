"""Prepare small-table decision evidence and record the user's source confirmation."""
import calendar
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "p0_2_audit" / "p0_2_v1"


def csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    manifest = ROOT / "manifests" / "raw_manifest.json"
    source_confirmation = {
        "status": "USER_CONFIRMED", "recorded_at_sgt": datetime.now(timezone(timedelta(hours=8))).isoformat(),
        "scope": "Current 2017-2024 annual CSVs, including 2018 and other whole-day gaps",
        "user_statement": "文件确认是官方导出且完整获取",
        "evidence_type": "Direct human confirmation in this conversation; not an independent remote checksum comparison",
        "raw_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "download_completeness_question": "RESOLVED_BY_USER",
        "gap_handling": "Treat absent records as gaps in the supplied official export; physical cause of gaps remains unknown",
        "source_files_modified": False,
    }
    confirmation_path = ROOT / "configs" / "source_confirmation.json"
    if confirmation_path.exists():
        previous = json.loads(confirmation_path.read_text(encoding="utf-8"))
        if previous["raw_manifest_sha256"] != source_confirmation["raw_manifest_sha256"]:
            raise RuntimeError("Source manifest changed; this existing human confirmation does not cover replacement files")
        source_confirmation = previous
    else:
        save(confirmation_path, source_confirmation)
    summary = json.loads((OUT / "summary.json").read_text(encoding="utf-8"))
    diagnostic = json.loads((OUT / "time_alignment_diagnostic" / "diagnostic.json").read_text(encoding="utf-8"))
    rows = csv_rows(OUT / "station_month_coverage.csv")
    common = {r["station_id"] for r in csv_rows(OUT / "station_full_period_summary.csv") if int(r["year_count"]) == 8}
    coverage = {(r["station_id"], int(r["year"]), int(r["month"])): float(r["calendar_coverage_ratio"]) for r in rows}
    eligibility = {}
    for threshold in [0.8, 0.9, 0.95, 0.99]:
        eligibility[str(threshold)] = {
            "eligible_common_station_months": sum(r["station_id"] in common and float(r["calendar_coverage_ratio"]) >= threshold for r in rows),
            "same_calendar_month_station_intersection_all_eight_years": {
                str(month): sorted(s for s in common if all(coverage[(s, year, month)] >= threshold for year in range(2017, 2025)))
                for month in range(1, 13)},
        }
    collision_records = csv_rows(OUT / "time_alignment_diagnostic" / "proposed_alignment_collision_records.csv")
    count = summary["total_spark_record_rows"]
    support = {"scope": "Exact calculations from previously audited small tables; not new full-data cleaning",
               "source_audit_summary_sha256": hashlib.sha256((OUT / "summary.json").read_bytes()).hexdigest(),
               "raw_rows": count, "offset_299_rows": summary["annual_metrics"][0]["off_five_minute_grid_rows"],
               "candidate_counts_if_no_other_exclusions": {
                   "align_and_merge_all_86_groups": count - diagnostic["proposed_collision_excess_rows"],
                   "align_and_quarantine_all_collision_records": count - len(collision_records),
                   "align_merge_84_groups_quarantine_2_metadata_groups": count - 88},
               "collision_nonzero_raw_records": sum(float(r["rain_mm"]) > 0 for r in collision_records),
               "collision_rain_values_mm": sorted({float(r["rain_mm"]) for r in collision_records}),
               "colliding_stations": len({r["station_id"] for r in collision_records}),
               "common_station_count": len(common), "possible_common_station_months": len(common) * 96,
               "calendar_grid_rows_if_all_91_stations_all_eight_years": summary["station_count_full_period"] * sum((366 if calendar.isleap(y) else 365) * 288 for y in range(2017, 2025)),
               "coverage_eligibility": eligibility}
    save(ROOT / "outputs" / "p0_3_decision_support.json", support)
    options = {
        "T": ("2017时间", "T2", ["T1", "T2", "T3"]),
        "C": ("对齐碰撞", "C1", ["C1", "C2", "C3"]),
        "L": ("五分钟测量区间", "L1", ["L1", "L2"]),
        "D": ("日月小时归属", "D1", ["D1", "D2"]),
        "M": ("缺测存储", "M1", ["M1", "M2"]),
        "H": ("模型历史完整性", "H1", ["H1", "H2", "H3"]),
        "R": ("合法雨量与极值", "R1", ["R1", "R2"]),
        "S": ("站点元数据", "S1", ["S1", "S2"]),
        "B": ("Task 1比较口径", "B1", ["B1", "B2", "B3"]),
        "K": ("Task 1月覆盖门槛", "K1", ["K1", "K2", "K3"]),
        "A": ("预测数据可用性", "A1", ["A1", "A2", "A3"]),
        "E": ("时间划分", "E1", ["E1", "E2"]),
        "P": ("训练规模", "P1", ["P1", "P2", "P3"]),
        "V": ("阈值输入方案", "V1", ["V1", "V2"]),
        "I": ("预测接口边界", "I1", ["I1", "I2"]),
    }
    pending = {"status": "PENDING_USER_DECISIONS", "applied": False,
               "source_confirmation": "configs/source_confirmation.json",
               "decision_document": "docs/P0-3及后续决策选项.md",
               "items": {key: {"topic": topic, "recommended": rec, "options": choices, "selected": None}
                         for key, (topic, rec, choices) in options.items()},
               "mandatory_rules": ["keep immutable raw files and lineage", "missing rainfall is unknown", "strictly greater than threshold",
                                   "no unresolved or missing future slots in primary labels", "chronological split and no test-label selection",
                                   "raw event_ts is preserved even when aligned timestamps are derived"],
               "dependencies": ["C applies only to T2", "T1 needs native-phase prediction anchors; fixed grid cannot be silently substituted",
                                "L2 requires interval-start labels and completed-only historical features",
                                "B/K affect Task1 analytical views, not global observation retention or all Task2 samples",
                                "H2 removes complete-60min features; H3 uses observed-sum names/counts rather than complete-window claims"],
               "later_parameter_rules": {"V1_initial_training_thresholds_mm": [0, 0.5, 1, 2, 5],
                                         "threshold_range_confirmation": "Check training-only valid-window support; freeze numeric range before validation/test evaluation",
                                         "decimal_precision": "Choose a lossless scale after raw precision audit; no unapproved rounding",
                                         "model_hyperparameters": "Implementation detail chosen on training/validation data, not a cleaning decision"}}
    pending_path = ROOT / "configs" / "project_decisions.pending.json"
    if pending_path.exists():
        previous = json.loads(pending_path.read_text(encoding="utf-8"))
        if previous.get("applied") or any(item.get("selected") is not None for item in previous["items"].values()):
            raise RuntimeError("Existing selections must not be reset by a decision-preparation script")
    save(pending_path, pending)
    print(json.dumps({"source_confirmation": source_confirmation["status"], "decision_items": len(options),
                      "candidate_rows": support["candidate_counts_if_no_other_exclusions"],
                      "monthly_common_station_intersections_90pct": {m: len(s) for m, s in eligibility["0.9"]["same_calendar_month_station_intersection_all_eight_years"].items()}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
