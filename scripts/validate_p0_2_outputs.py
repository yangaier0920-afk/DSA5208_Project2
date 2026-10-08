"""Reconcile delivered small CSVs with annual Spark audit evidence and hash them."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "p0_2_audit" / "p0_2_v1"


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    global OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=OUT)
    parser.add_argument("--self-test-dir", type=Path, default=ROOT / "outputs" / "p0_2_audit" / "self_test_v1")
    args = parser.parse_args()
    OUT = args.audit_dir.resolve()
    state = load(OUT / "run_status.json")
    summary = load(OUT / "summary.json")
    checks = {}
    checks["run_and_shutdown_pass"] = state["status"] == summary["status"] == "PASS" and state["shutdown"]["gateway_exit_code"] == 0
    checks["all_eight_years_completed"] = state["completed_years"] == list(range(2017, 2025))
    checks["audit_source_code_matches_run"] = digest(ROOT / "src" / "01_audit_raw.py") == state["signature"]["code_sha256"]
    checks["session_helper_matches_run"] = digest(ROOT / "scripts" / "spark_session.py") == state["signature"]["session_helper_sha256"]
    checks["manifest_matches_run"] = digest(ROOT / "manifests" / "raw_manifest.json") == state["signature"]["manifest_sha256"]
    annual = [load(OUT / str(year) / "audit.json") for year in range(2017, 2025)]
    checks["all_80_annual_reconciliations"] = all(len(result["checks"]) == 10 and all(result["checks"].values()) for result in annual)
    checks["summary_matches_annual_metrics"] = summary["annual_metrics"] == [result["metrics"] for result in annual]
    checks["annual_csv_counts_match"] = [int(row["spark_record_rows"]) for row in csv_rows(OUT / "annual_quality.csv")] == [r["metrics"]["spark_record_rows"] for r in annual]
    checks["total_count_matches_manifest_physical_counts"] = summary["total_spark_record_rows"] == sum(s["physical_data_lines"] for s in summary["sources"])
    checks["source_sizes_mtimes_unchanged"] = all((ROOT / s["path"]).stat().st_size == s["bytes"] and (ROOT / s["path"]).stat().st_mtime_ns == s["mtime_ns"] for s in summary["sources"])
    checks["disjoint_years_establish_no_interfile_key_overlap"] = all(r["metrics"]["event_year_mismatch_rows"] == r["metrics"]["invalid_timestamp_rows"] == r["metrics"]["missing_timestamp_rows"] == 0 for r in annual)
    for name in annual[0]["tables"]:
        expected = [row for result in annual for row in result["tables"][name]]
        actual = csv_rows(OUT / (name + ".csv"))
        checks["csv_export_" + name] = len(expected) == len(actual) and all(
            {k: "" if v is None else str(v) for k, v in e.items()} == a for e, a in zip(expected, actual))
    coverage = csv_rows(OUT / "station_month_coverage.csv")
    checks["coverage_dimensions"] = len(coverage) == sum(r["metrics"]["stations"] * 12 for r in annual)
    checks["full_period_station_summary_rows"] = len(csv_rows(OUT / "station_full_period_summary.csv")) == summary["station_count_full_period"]
    daily = csv_rows(OUT / "daily_network_record_counts.csv")
    checks["calendar_daily_export_reconciles"] = len(daily) == 2922 and sum(int(r["raw_record_count"]) for r in daily) == summary["total_spark_record_rows"]
    network = csv_rows(OUT / "common_station_month_network_coverage.csv")
    common = {r["station_id"] for r in csv_rows(OUT / "station_full_period_summary.csv") if int(r["year_count"]) == 8}
    checks["common_network_export_reconciles"] = len(network) == 96 and sum(int(r["observed_nominal_slots"]) for r in network) == sum(int(r["occupied_nominal_slots"]) for r in coverage if r["station_id"] in common)
    self_test = load(args.self_test_dir.resolve() / "self_test.json")
    checks["synthetic_anomaly_tests_pass"] = self_test["status"] == "PASS" and all(r["passed"] for r in self_test["checks"].values())
    diagnostic = load(OUT / "time_alignment_diagnostic" / "diagnostic.json")
    checks["diagnostic_source_code_matches_run"] = digest(ROOT / "src" / "01b_audit_time_alignment.py") == diagnostic["code_sha256"]
    checks["time_alignment_diagnostic_pass"] = diagnostic["status"] == "PASS" and diagnostic["shutdown"]["gateway_exit_code"] == 0 and all(diagnostic["checks"].values())
    records = csv_rows(OUT / "time_alignment_diagnostic" / "proposed_alignment_collision_records.csv")
    collision_groups = csv_rows(OUT / "time_alignment_diagnostic" / "proposed_alignment_collision_groups.csv")
    checks["diagnostic_collision_export"] = len(collision_groups) == diagnostic["proposed_collision_groups"] and len(records) == diagnostic["proposed_collision_excess_rows"] + diagnostic["proposed_collision_groups"]
    checks["report_exists"] = (ROOT / "docs" / "p0_2_quality_report.md").is_file()
    validation = {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
                  "source_integrity_note": "Size/mtime unchanged; full source SHA256 was rechecked in P0-1, not recalculated by this validator.",
                  "report_sha256": digest(ROOT / "docs" / "p0_2_quality_report.md"),
                  "report_generator_sha256": digest(Path(__file__).parent / "summarize_p0_2.py"),
                  "validator_sha256": digest(Path(__file__))}
    (OUT / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    assert all(checks.values()), {k: v for k, v in checks.items() if not v}
    outputs = [{"path": path.relative_to(ROOT).as_posix(), "bytes": path.stat().st_size, "sha256": digest(path)}
               for path in sorted(OUT.rglob("*")) if path.is_file() and path.name != "output_manifest.json"]
    (OUT / "output_manifest.json").write_text(json.dumps({"status": "PASS", "files": outputs}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"PASS: {len(checks)} delivery checks; {len(outputs)} output files hashed")


if __name__ == "__main__":
    main()
