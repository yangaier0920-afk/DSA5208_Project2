"""Build a Chinese review report from completed P0-2 Spark outputs (small tables)."""
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
    source_note = ("**来源完整性已由用户确认**：当前文件为官方导出且完整获取；确认记录见 `configs/source_confirmation.json`。因此把无记录日期按当前官方导出资料中的缺测处理，保留原文件与 manifest。该确认来自用户，不表示重新与官方远程文件做了哈希比对；缺测的物理原因仍未知。"
                   if source_confirmed else
                   "**A 的下一步先核对 2018 年及其他整日缺口的官方导出/获取完整性**，保留当前文件与 manifest，不直接覆盖原始资料。本地审计不能单独判定缺口来自官方历史资料还是下载获取过程。如果换取更完整版本，必须记录新来源/hash，并重跑审计。")
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
    lines = ["# P0-2 全量质量审计结果", "", f"生成时间：{summary['completed_at_sgt']}。", "",
             "**2017—2024 全部年度文件已经实际执行 Spark 审计，八年对账检查及 Java 正常退出检查通过。原始 CSV 未修改。**", "",
             f"Spark 尝试解析 **{summary['total_spark_record_rows']:,}** 条记录，结构合格 **{summary['total_structurally_valid_rows']:,}** 条，年度文件内唯一站点时间键合计 **{summary['total_unique_station_time_keys_within_files']:,}** 个。全期站点 **{summary['station_count_full_period']}** 个。候选有效记录 **{summary['total_candidate_valid_rows']:,}** 条；该口径尚不等于正式清洗交付数。", "",
             f"候选有效的五分钟观测中，雨量大于零共有 **{sum(m['positive_candidate_rows'] for m in annual):,}** 条，占 **{sum(m['positive_candidate_rows'] for m in annual) / summary['total_candidate_valid_rows']:.2%}**。这是单条五分钟观测的正雨量比例，不是未来 30 分钟超阈值标签的正例率；C 应在拿到完整窗口标签后重新计算类别分布。", "",
             "## 1. 年度正式规模与主要问题", "",
             "| 年度 | Spark 记录数 | 站点数 | 唯一时间键 | 重复键多余行 | 雨量冲突键 | 偏离标准网格行 | >300秒相邻间隔 |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for m in annual:
        lines.append(f"| {m['year']} | {m['spark_record_rows']:,} | {m['stations']} | {m['unique_station_time_keys']:,} | {m['duplicate_key_excess_rows']:,} | {m['conflicting_reading_key_groups']:,} | {m['off_five_minute_grid_rows']:,} | {m['gap_gt_300s_intervals']:,} |")
    issue_fields = ["csv_corrupt", "csv_field_count_mismatch", "csv_odd_quotes", "missing_station_id", "missing_rainfall",
                    "invalid_rainfall_number", "nonfinite_rainfall", "negative_rainfall", "unexpected_unit", "unexpected_reading_type",
                    "invalid_timestamp", "missing_timestamp", "event_year_mismatch", "date_mismatch_or_missing", "invalid_coordinates"]
    lines += ["", "| 检查条件 | 八年涉及行数 |", "|---|---:|"]
    for field in issue_fields:
        lines.append(f"| `{field}` | {sum(m[field + '_rows'] for m in annual):,} |")
    lines += ["", "检查条件可重叠，不能将异常行数直接相加作为清洗删除量。每年详细时间字段、设备/站点名缺失及数值极值见 `annual_quality.csv`。正式规模是 Spark 全量统计，并与此前物理行清单逐年一致；来源及文件身份以 `manifests/raw_manifest.json` 为准。", "",
              "## 2. 时间偏移和间隔", ""]
    for m in annual:
        offsets = [r for r in read_csv("timestamp_offsets.csv") if int(r["year"]) == m["year"]]
        values = "；".join(f"offset={r['offset_s'] or '未解析'} 秒：{int(r['count']):,} 行" for r in offsets)
        lines.append(f"- {m['year']}：{values}；小于 300 秒的相邻间隔 {m['gap_lt_300s_intervals']:,} 次，非 300 整数倍的间隔 {m['gap_not_multiple_300s_intervals']:,} 次。")
    shifted_2017 = [r for r in daily if r["year"] == "2017" and r["offset_s"] and int(r["offset_s"]) != 0]
    if shifted_2017:
        lines += ["", f"2017 的偏移在全量数据中出现于 {min(r['event_date'] for r in shifted_2017)} 至 {max(r['event_date'] for r in shifted_2017)}。每日分布保存在 `daily_timestamp_offsets.csv`；这描述出现范围，不能据此假设所有站点同一时刻切换。"]
    collisions = sum(int(r["slot_collision_excess"]) for r in coverage)
    lines += ["", f"全年站点月表共记录 **{collisions:,}** 个“唯一秒级时间数超出占用诊断槽位数”的差额。槽位 floor 操作仅用于审计：保留原始时间，先定位站点与切换边界，再决定下一阶段的对齐规则。没有直接给 2017 全部时间加一秒。",
              "", "30 分钟标签必须按时间跨度判断观测完整性，不能直接取后六行；原始时间、候选对齐时间和缺口应可追溯。跨年间隔另见 `year_boundary_gaps.csv`。", "",
              "## 3. 站点覆盖与元数据", "",
              f"station-month 表共 **{len(coverage):,}** 行，包括当年出现过的站点的零观测月份。完整月覆盖率为 0 的 station-month 有 **{sum(float(r['calendar_coverage_ratio']) == 0 for r in coverage):,}** 个；低于 95% 的有 **{sum(float(r['calendar_coverage_ratio']) < 0.95 for r in coverage):,}** 个。这些数量包含站点启停范围，不能全部叫作设备漏测。", "",
              f"八年都出现过的站点共 **{len(common)}** 个：{', '.join(common)}。这还不是正式固定比较面板。", "",
              "为 B 的跨年比较提供三个候选敏感性口径：要求站点八年都出现、96 个月中每个月的完整月槽位覆盖率都达标。", "",
              "| 每月最低覆盖率 | 候选站点数 | 候选站点 |", "|---|---:|---|"]
    for threshold, panel in panels.items():
        lines.append(f"| {float(threshold):.0%} | {len(panel)} | {', '.join(panel) or '无'} |")
    lowest = sorted(network_rows, key=lambda r: r["coverage_ratio"])[:8]
    panel_note = ("**重要：三个严格全月面板均为空，因此不能直接采用原先设想的‘八年固定站点且每月覆盖至少90%’主口径。**"
                  if not any(panels.values()) else "候选面板仍需核对元数据与缺口，覆盖门槛尚未确定。")
    lines += ["", panel_note,
              "", "38 个共同站点中覆盖最低的月份如下，分母固定为 38 个站点×完整月槽位：", "",
              "| 年月 | 共同站点槽位覆盖率 |", "|---|---:|"]
    for row in lowest:
        lines.append(f"| {row['year']}-{row['month']:02d} | {row['coverage_ratio']:.2%} |")
    august = [row for row in day_rows if row["year"] == 2018 and row["event_date"].startswith("2018-08") and row["raw_record_count"] > 0]
    august_gap = ("8 月 4—31 日没有记录；这不是这些日期没有下雨。"
                  if [r["event_date"] for r in august] == ["2018-08-01", "2018-08-02", "2018-08-03"]
                  else "缺少记录的日期不能当作无雨日期。")
    lines += ["", f"本地原始 2018 CSV 在 8 月有 **{len(august)} 天**存在任何站点记录：{', '.join(row['event_date'] for row in august)}。{august_gap}全期共有 **{len(missing_days)} 个日历日**在对应年度 CSV 中没有任何站点记录，详见 `daily_network_record_counts.csv`。", "",
              source_note, "",
              "针对资料缺测，B 应明确分析的是实际观测时段，按有效观测统计频率/强度并展示覆盖，必要时按可比的月/时段做分期分析；不能把缺测当零、直接解释全年/月累计为真实总雨量，或仅靠全月总量除覆盖率修补。C 的标签和滞后特征应按连续片段生成，整段缺口附近的窗口须标为不完整。"]
    lines += ["", "这是可比较性候选与敏感性检查，不是已经采用的筛选阈值。即使覆盖达标，也要核对坐标变化、异常雨量和季节性缺口；若面板过小，可改为明确的站点内对比、覆盖标准化和分期面板。不要直接把所有站点雨量相加比较年度。", "",
              "| 全期元数据检查 | 站点数 |", "|---|---:|"]
    for label, field in [("名称有多个原文", "name_variants"), ("device ID 有多个原文", "device_variants"), ("数字经纬度有多个组合", "numeric_coordinate_variants")]:
        lines.append(f"| {label} | {sum(int(r[field]) > 1 for r in stations)} |")
    changed = [r for r in stations if int(r["numeric_coordinate_variants"]) > 1]
    lines += ["", "坐标变体站点：" + (", ".join(r["station_id"] for r in changed) or "无") + "。",
              "原始组合的首次/末次出现范围及计数见 `station_metadata_history.csv`；不能把坐标版本变化自动当成实地搬迁。事实表应以 station_id 关联元数据历史，名称不作主键，不能用最新名称/坐标覆盖所有历史。", "",
              "## 4. 更新延迟和雨量极值", "",
              "| 年度 | 整体更新>1天行数 | 雨量更新>1天行数 | 整体更新负延迟 | 雨量更新负延迟 | 最大有限雨量(mm) |", "|---|---:|---:|---:|---:|---:|"]
    for m in annual:
        lines.append(f"| {m['year']} | {m['update_delay_gt_day_rows']:,} | {m['reading_update_delay_gt_day_rows']:,} | {m['negative_update_delay_rows']:,} | {m['negative_reading_update_delay_rows']:,} | {m['rain_max_finite_mm']} |")
    lines += ["", "更新字段减观测时间是记录字段差值，并不证明实时首次可用延迟。保留两个更新时间供追踪，不以晚更新为理由删除历史有效雨量；C 的回测需要在报告中说明历史观测可用性假设，不能用未来更新状态做特征。", "",
              "原始最大雨量示例见 `largest_rainfall_examples.csv`。非 0.2 倍数、多位小数和极端值不能仅凭形式删除；如要设异常阈值，需要先确认观测仪器/累积口径、相邻时间和邻站证据，规则及影响应另行记录。", "",
              "## 5. 下一步 A/B/C 的接口", "",
              "- **A / P0-3**：先裁决秒偏移与槽位碰撞、重复与冲突、元数据映射、有效观测范围。实现标准化观测 Parquet，逐条保留来源年/原始时间/质量标志，输出原始→异常→去重→保留的账目。缺测保持缺测；下一阶段再生成完整未来窗口标签。",
              "- **B**：现在可先使用 station-month 与元数据表确定覆盖图和比较口径；年际对比采用明确的面板或覆盖控制。清洗事实表确定后再正式计算季节性、小时变化与年际结论。",
              "- **C**：继续实现基线、时间切分和 Spark MLlib 概率接口。拿到 A 的有效窗口后检查标签比例和站点/月差异；存在时间缺口的未来窗口不能变成零雨量负例。训练、验证、测试接口需共同确定，不在本审计中自动锁定。", "",
              "B/C 的跨机器小样本验收按用户要求暂缓，不影响本次 A 的全量审计验收。本阶段未生成清洗数据、标签或模型。", "",
              "## 6. 交付与复现证据", "",
              f"结果根目录：`{OUT.relative_to(ROOT).as_posix()}/`。", "",
              "- `annual_quality.csv` / `summary.json`：正式规模和年度完整质量指标。",
              "- `station_month_coverage.csv`：站点×月份覆盖；`station_summary.csv` / `station_full_period_summary.csv`：年度/全期站点汇总。",
              "- `common_station_month_network_coverage.csv` / `daily_network_record_counts.csv`：共同站点月度网络覆盖、全日历日期原始记录数（含零记录日）。",
              "- `station_metadata_history.csv`：元数据组合的出现历史。",
              "- `timestamp_offsets.csv` / `daily_timestamp_offsets.csv` / `cadence_gap_counts.csv` / `year_boundary_gaps.csv`：时间刻度与连续性。",
              "- `units_types.csv` / `update_delay_buckets.csv`：单位、类型及更新字段差值分布。",
              "- `anomaly_examples.csv` / `duplicate_key_examples.csv` / `cadence_gap_examples.csv` / `largest_rainfall_examples.csv`：定位样例。",
              "- 每个年度目录 `audit.json`：年度指标、小表及 10 项对账检查；`run_status.json`：资源、代码/输入签名及进程退出结果。",
              "- `outputs/p0_2_audit/self_test_v1/self_test.json`：合成异常测试结果。", "",
              "程序：`src/01_audit_raw.py`，启动/资源生命周期：`scripts/run_spark.py`、`scripts/spark_session.py`。口径及重跑命令见 [p0_2_methodology.md](p0_2_methodology.md)。", ""]
    diagnostic_path = OUT / "time_alignment_diagnostic" / "diagnostic.json"
    if diagnostic_path.exists():
        diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
        assert diagnostic["status"] == "PASS" and diagnostic["shutdown"]["passed"]
        position = lines.index("## 3. 站点覆盖与元数据")
        lines[position:position] = [
            "### 2017 对齐规则的补充审计（未执行清洗）", "",
            f"逐站点排序确认 **{diagnostic['phase_transition_events']}** 次偏移相位切换和 **{diagnostic['sub_300s_pairs']}** 个不足 300 秒的相邻观测对，保存了全部这些小表记录。",
            f"若仅将 offset=299 的时间加 1 秒，会产生 **{diagnostic['proposed_collision_groups']}** 组重复目标键、**{diagnostic['proposed_collision_excess_rows']}** 条重复多余行；其中 **{diagnostic['proposed_numeric_rainfall_conflicts']}** 组雨量数值不一致，**{diagnostic['proposed_metadata_conflicts']}** 组元数据不一致。另有 **{diagnostic['proposed_date_changes']:,}** 条记录的本地日历日期会改变。", "",
            "这说明时间修正与去重/冲突规则必须一起设计。所有碰撞组、原始记录和相位切换事件保存在 `time_alignment_diagnostic/`；本次没有采用这条候选规则，也没有从碰撞组中挑选某条保留。", "",
        ]
    (ROOT / "docs" / "p0_2_quality_report.md").write_text("\n".join(lines), encoding="utf-8")
    proposal = {"not_an_approved_panel": True, "definition": "station observed in all 8 years; every one of 96 months reaches calendar nominal-slot coverage threshold", "common_stations": common, "candidate_panels": panels}
    (OUT / "comparability_panel_candidates.json").write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Report written: {ROOT / 'docs' / 'p0_2_quality_report.md'}")


if __name__ == "__main__":
    main()
