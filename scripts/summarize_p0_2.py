"""Build a English review report from completed P0-2 Spark outputs (small tables)."""
import argparse
import calendar
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "p0_2_audit" / "p0_2_v1"


def read_csv(name):
    with (OUT / name).open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def local_time(value):
    return datetime.fromtimestamp(int(value), timezone(timedelta(hours=8))).isoformat() if value else "—"


def main():
    global OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=OUT)
    args = parser.parse_args()
    OUT = args.audit_dir.resolve()
    summary = json.loads((OUT / "summary.json").read_text(encoding="utf-8"))
    state = json.loads((OUT / "run_status.json").read_text(encoding="utf-8"))
    assert summary["status"] == state["status"] == "PASS" and state["shutdown"]["passed"]
    annual = summary["annual_metrics"]
    assert len(annual) == 8 and summary["all_reconciliation_checks_passed"]
    confirmation_path = ROOT / "configs" / "source_confirmation.json"
    confirmation = json.loads(confirmation_path.read_text(encoding="utf-8")) if confirmation_path.exists() else None
    source_confirmed = bool(confirmation and confirmation["status"] == "USER_CONFIRMED" and
                            confirmation["raw_manifest_sha256"] == state["signature"]["manifest_sha256"])
    source_note = ("**Source completeness was confirmed by the user** in configs/source_confirmation.json. Treat absent dates as gaps in the supplied official export, preserving original files/manifest. This is human confirmation, not a remote official-file hash comparison; the physical cause remains unknown."
                   if source_confirmed else
                   "**A must first confirm official-export/acquisition completeness for 2018 and other whole-day gaps**. Preserve files/manifest. Local audit cannot distinguish historical-source gaps from acquisition problems. Replacement data requires a new source/hash and a fresh audit.")
    # Empty result tables still expose a stable schema to pandas/CSV readers.
    empty_schemas = {
        "duplicate_key_examples": ["year", "station_id", "epoch_s", "n", "distinct_full_rows", "distinct_readings", "candidate_rows"],
        "anomaly_examples": ["year", "reason", "date", "timestamp", "update_timestamp", "station_id", "station_name", "station_device_id", "location_longitude", "location_latitude", "reading_update_timestamp", "reading_value", "reading_type", "reading_unit", "field_count", "_corrupt_record", "raw_line"],
        "cadence_gap_examples": ["year", "station_id", "epoch_s", "candidate_rows", "event_ts", "event_year", "month", "offset_s", "slot", "previous_epoch_s", "gap_s"],
    }
    for folder in [OUT, *[OUT / str(m["year"]) for m in annual]]:
        for name, columns in empty_schemas.items():
            path = folder / (name + ".csv")
            if path.exists() and not path.read_text(encoding="utf-8-sig").strip():
                with path.open("w", encoding="utf-8-sig", newline="") as stream:
                    csv.writer(stream).writerow(columns)
    coverage = read_csv("station_month_coverage.csv")
    stations = read_csv("station_full_period_summary.csv")
    daily = read_csv("daily_timestamp_offsets.csv")
    metadata = read_csv("station_metadata_history.csv")
    duplicates = read_csv("duplicate_key_examples.csv")
    max_rain = read_csv("largest_rainfall_examples.csv")
    station_months = defaultdict(list)
    for row in coverage:
        station_months[row["station_id"]].append(row)
    common = sorted(r["station_id"] for r in stations if int(r["year_count"]) == 8)
    panels = {str(threshold): sorted(s for s in common if len(station_months[s]) == 96 and
                                   all(float(r["calendar_coverage_ratio"]) >= threshold for r in station_months[s]))
              for threshold in [0.9, 0.95, 0.99]}
    network = defaultdict(lambda: [0, 0, 0])
    for row in coverage:
        if row["station_id"] in common:
            key = (int(row["year"]), int(row["month"]))
            network[key][0] += int(row["occupied_nominal_slots"])
            network[key][1] += int(row["expected_calendar_slots"])
            network[key][2] += 1
    network_rows = [{"year": year, "month": month, "common_station_count": n,
                     "observed_nominal_slots": observed, "expected_nominal_slots": expected,
                     "coverage_ratio": observed / expected} for (year, month), (observed, expected, n) in sorted(network.items())]
    with (OUT / "common_station_month_network_coverage.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(network_rows[0])); writer.writeheader(); writer.writerows(network_rows)
    daily_counts = defaultdict(int)
    for row in daily:
        daily_counts[(int(row["year"]), row["event_date"])] += int(row["count"])
    day_rows = []
    for year in range(2017, 2025):
        for month in range(1, 13):
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                date = f"{year:04d}-{month:02d}-{day:02d}"
                day_rows.append({"year": year, "event_date": date, "raw_record_count": daily_counts[(year, date)],
                                 "network_has_records": daily_counts[(year, date)] > 0})
    with (OUT / "daily_network_record_counts.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(day_rows[0])); writer.writeheader(); writer.writerows(day_rows)
    missing_days = [row for row in day_rows if row["raw_record_count"] == 0]
    lines = ["# P0-2 full quality audit results", "", f"Generated at: {summary['completed_at_sgt']}。", "",
             "**All 2017–2024 annual exports were actually audited using Spark; eight-year reconciliation and normal Java shutdown passed. Raw CSV files were not modified.**", "",
             f"Spark attempted to parse **{summary['total_spark_record_rows']:,}** records; structurally valid **{summary['total_structurally_valid_rows']:,}**; annual unique station/time keys total **{summary['total_unique_station_time_keys_within_files']:,}**. Full-period stations **{summary['station_count_full_period']}**. Candidate valid records **{summary['total_candidate_valid_rows']:,}**; this is not yet the formal cleaned delivery count.", "",
             f"Candidate valid five-minute observations with positive rain total **{sum(m['positive_candidate_rows'] for m in annual):,}** rows, proportion **{sum(m['positive_candidate_rows'] for m in annual) / summary['total_candidate_valid_rows']:.2%}**. This is single-slot wet frequency, not future-30-minute exceedance prevalence; C must recompute class support using complete truth.", "",
             "## 1. Full annual counts and main issues", "",
             "| Year | Spark rows | Stations | Unique time keys | Duplicate excess rows | Conflicting-rain keys | Off-grid rows | Adjacent gaps >300s ||",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for m in annual:
        lines.append(f"| {m['year']} | {m['spark_record_rows']:,} | {m['stations']} | {m['unique_station_time_keys']:,} | {m['duplicate_key_excess_rows']:,} | {m['conflicting_reading_key_groups']:,} | {m['off_five_minute_grid_rows']:,} | {m['gap_gt_300s_intervals']:,} |")
    issue_fields = ["csv_corrupt", "csv_field_count_mismatch", "csv_odd_quotes", "missing_station_id", "missing_rainfall",
                    "invalid_rainfall_number", "nonfinite_rainfall", "negative_rainfall", "unexpected_unit", "unexpected_reading_type",
                    "invalid_timestamp", "missing_timestamp", "event_year_mismatch", "date_mismatch_or_missing", "invalid_coordinates"]
    lines += ["", "| Check | Eight-year affected rows ||", "|---|---:|"]
    for field in issue_fields:
        lines.append(f"| `{field}` | {sum(m[field + '_rows'] for m in annual):,} |")
    lines += ["", "Checks overlap; affected rows cannot be added as cleaning losses. Annual timestamp, missing device/name and extreme-value details are in annual_quality.csv. Full Spark counts agree annually with the physical-line inventory; manifests/raw_manifest.json identifies input source/bytes.", "",
              "## 2. Timestamp phases and cadence", ""]
    for m in annual:
        offsets = [r for r in read_csv("timestamp_offsets.csv") if int(r["year"]) == m["year"]]
        values = "；".join(f"offset={r['offset_s'] or 'Unparsed'} seconds: {int(r['count']):,} rows" for r in offsets)
        lines.append(f"- {m['year']}：{values}; adjacent gaps below 300 seconds {m['gap_lt_300s_intervals']:,}; nonmultiples of 300 seconds {m['gap_not_multiple_300s_intervals']:,} occurrences.")
    shifted_2017 = [r for r in daily if r["year"] == "2017" and r["offset_s"] and int(r["offset_s"]) != 0]
    if shifted_2017:
        lines += ["", f"2017 offsets appear in the full data from {min(r['event_date'] for r in shifted_2017)} to {max(r['event_date'] for r in shifted_2017)}. Daily distribution is in daily_timestamp_offsets.csv. Appearance range does not establish simultaneous station transitions."]
    collisions = sum(int(r["slot_collision_excess"]) for r in coverage)
    lines += ["", f"Station-month tables contain **{collisions:,}** excess unique-second times over occupied diagnostic slots. Flooring is diagnostic only; retain original times, identify stations/transitions before choosing alignment. The audit did not shift every 2017 timestamp.",
              "", "30-minute truth requires exact time-span completeness, never simply the next six rows. Preserve original/candidate-aligned times and gaps. Cross-year cadence is in year_boundary_gaps.csv.", "",
              "## 3. Station coverage and metadata", "",
              f"Station-month table has **{len(coverage):,}** rows including zero-observation months for stations present that year. Zero-coverage station-months **{sum(float(r['calendar_coverage_ratio']) == 0 for r in coverage):,}**; below 95% **{sum(float(r['calendar_coverage_ratio']) < 0.95 for r in coverage):,}**. Counts include station start/stop scope, not only equipment missed readings.", "",
              f"Stations appearing in all eight years total **{len(common)}**: {', '.join(common)}. This is not yet a confirmed fixed comparison panel.", "",
              "Three candidate sensitivity panels for B require station presence in all eight years and qualifying complete-calendar coverage in each of 96 months.", "",
              "| Minimum monthly coverage | Candidate count | Candidate stations ||", "|---|---:|---|"]
    for threshold, panel in panels.items():
        lines.append(f"| {float(threshold):.0%} | {len(panel)} | {', '.join(panel) or 'None'} |")
    lowest = sorted(network_rows, key=lambda r: r["coverage_ratio"])[:8]
    panel_note = ("**All three strict all-month panels are empty, so an eight-year fixed station set with every month >=90% is unsupported.**"
                  if not any(panels.values()) else "Candidate panels still require metadata/gap review; the coverage cutoff was not yet confirmed at this historical stage.")
    lines += ["", panel_note,
              "", "Lowest-coverage months among 38 common stations follow; denominator is 38 stations times complete-calendar monthly slots:", "",
              "| Year-month | Common-station slot coverage ||", "|---|---:|"]
    for row in lowest:
        lines.append(f"| {row['year']}-{row['month']:02d} | {row['coverage_ratio']:.2%} |")
    august = [row for row in day_rows if row["year"] == 2018 and row["event_date"].startswith("2018-08") and row["raw_record_count"] > 0]
    august_gap = ("4–31 August has no records; this does not mean no rainfall."
                  if [r["event_date"] for r in august] == ["2018-08-01", "2018-08-02", "2018-08-03"]
                  else "Dates without records are not dry dates.")
    lines += ["", f"The local raw 2018 CSV has **{len(august)} days** with any August station records: {', '.join(row['event_date'] for row in august)}。{august_gap}Full-period total **{len(missing_days)} calendar days** with no station record in the corresponding annual CSV; see daily_network_record_counts.csv.", "",
              source_note, "",
              "B must analyze observed periods, compute frequency/intensity from valid readings, display coverage and use comparable months/periods. Do not zero-fill gaps, call incomplete sums true totals or repair them by dividing by coverage. C truth and lag features require continuous exact-time segments; windows near gaps are incomplete."]
    lines += ["", "These are candidate comparability/sensitivity checks, not confirmed filters. Review coordinates, unusual rain and seasonal gaps even with adequate coverage. Small panels may require explicit within-station or period comparisons. Do not compare annual sums across all changing stations.", "",
              "| Full-period metadata check | Stations ||", "|---|---:|"]
    for label, field in [("Multiple original station names", "name_variants"), ("Multiple original device IDs", "device_variants"), ("Multiple numeric coordinate combinations", "numeric_coordinate_variants")]:
        lines.append(f"| {label} | {sum(int(r[field]) > 1 for r in stations)} |")
    changed = [r for r in stations if int(r["numeric_coordinate_variants"]) > 1]
    lines += ["", "Stations with coordinate variants: " + (", ".join(r["station_id"] for r in changed) or "None") + "。",
              "Combination first/last appearances and counts are in station_metadata_history.csv; coordinate variation does not establish physical relocation. Join historical metadata by station_id, not name, and never overwrite all history with latest names/coordinates.", "",
              "## 4. Update differences and rainfall extremes", "",
              "| Year | Update >1 day rows | Reading update >1 day | Negative update delay | Negative reading delay | Maximum finite rain/mm ||", "|---|---:|---:|---:|---:|---:|"]
    for m in annual:
        lines.append(f"| {m['year']} | {m['update_delay_gt_day_rows']:,} | {m['reading_update_delay_gt_day_rows']:,} | {m['negative_update_delay_rows']:,} | {m['negative_reading_update_delay_rows']:,} | {m['rain_max_finite_mm']} |")
    lines += ["", "Update minus event time is a field difference, not proof of first real-time availability. Retain both updates and valid delayed rain. C must disclose retrospective availability assumptions and exclude future update status from features.", "",
              "See largest_rainfall_examples.csv. Values not divisible by 0.2, long decimals and extremes cannot be removed on appearance alone. An anomaly cutoff requires instrument/accumulation semantics, adjacent-time/station evidence and a documented rule/impact.", "",
              "## 5. Next A/B/C interfaces", "",
              "- **A/P0-3**: confirm phase/collision, duplicate/conflict, metadata and validity rules. Write standardized Parquet with source year/original time/quality and raw-to-quarantine/dedup/retained ledgers. Preserve gaps; construct future truth in the next stage.",
              "- **B**: use station-month/metadata tables for coverage plots and comparison policy. Adopt explicit interannual panels/coverage control; compute formal seasonal/hourly/interannual results after cleaned facts are frozen.",
              "- **C**: implement baseline, time partitions and Spark MLlib probability interface. Review training label/station/month support after A windows arrive. Gapped future windows are not dry negatives. Agree train/validation/test interfaces separately; this audit does not lock them automatically.", "",
              "B/C machine checks were deferred by the user without blocking A full audit. This stage created no cleaned data, targets or models.", "",
              "## 6. Delivery and reproduction evidence", "",
              f"Result root: `{OUT.relative_to(ROOT).as_posix()}/`。", "",
              "- annual_quality.csv / summary.json: full counts and annual quality.",
              "- station_month_coverage.csv: station/month coverage; station_summary.csv / station_full_period_summary.csv: annual/full-period station summaries.",
              "- common_station_month_network_coverage.csv / daily_network_record_counts.csv: common-network monthly coverage and full-calendar daily raw counts, including zero-record dates.",
              "- station_metadata_history.csv: observed metadata combination history.",
              "- timestamp_offsets.csv / daily_timestamp_offsets.csv / cadence_gap_counts.csv / year_boundary_gaps.csv: phases and continuity.",
              "- units_types.csv / update_delay_buckets.csv: units, types and update-field differences.",
              "- anomaly_examples.csv / duplicate_key_examples.csv / cadence_gap_examples.csv / largest_rainfall_examples.csv: diagnostic examples.",
              "- Annual audit.json: metrics, small tables and ten reconciliation checks; run_status.json: resources, code/input signatures and process exit.",
              "- outputs/p0_2_audit/self_test_v1/self_test.json: synthetic anomaly checks.", "",
              "Program: src/01_audit_raw.py; lifecycle: scripts/run_spark.py and spark_session.py. See [p0_2_methodology.md](p0_2_methodology.md) for policy/reproduction.", ""]
    diagnostic_path = OUT / "time_alignment_diagnostic" / "diagnostic.json"
    if diagnostic_path.exists():
        diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
        assert diagnostic["status"] == "PASS" and diagnostic["shutdown"]["passed"]
        position = lines.index("## 3. Station coverage and metadata")
        lines[position:position] = [
            "### Supplemental 2017 alignment audit (no cleaning performed)", "",
            f"Station-wise ordering confirmed **{diagnostic['phase_transition_events']}** phase transitions and **{diagnostic['sub_300s_pairs']}** adjacent observation pairs below 300 seconds; all small-table records are retained.",
            f"Adding one second only for offset=299 would produce **{diagnostic['proposed_collision_groups']}** duplicate target keys, **{diagnostic['proposed_collision_excess_rows']}** excess duplicate rows; including **{diagnostic['proposed_numeric_rainfall_conflicts']}** rain conflicts and **{diagnostic['proposed_metadata_conflicts']}** metadata conflicts. Another **{diagnostic['proposed_date_changes']:,}** records would change local calendar date.", "",
            "Alignment and collision rules must be designed jointly. All collision groups/raw rows/phase transitions are in time_alignment_diagnostic/. This audit did not adopt the candidate rule or choose retained collision rows.", "",
        ]
    (ROOT / "docs" / "p0_2_quality_report.md").write_text("\n".join(lines), encoding="utf-8")
    proposal = {"not_an_approved_panel": True, "definition": "station observed in all 8 years; every one of 96 months reaches calendar nominal-slot coverage threshold", "common_stations": common, "candidate_panels": panels}
    (OUT / "comparability_panel_candidates.json").write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Report written: {ROOT / 'docs' / 'p0_2_quality_report.md'}")


if __name__ == "__main__":
    main()
