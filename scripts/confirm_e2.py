"""Record the user's explicit E2 decision without marking any processing complete."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
pending_path = ROOT / 'configs/project_decisions.pending.json'
project_path = ROOT / 'configs/project.json'
pending = json.loads(pending_path.read_text(encoding='utf-8'))
project = json.loads(project_path.read_text(encoding='utf-8'))
assert all(v['selected'] is not None for k, v in pending['items'].items() if k != 'E')
assert pending['items']['E']['selected'] in (None, 'E2')
pending['items']['E'].update(selected='E2', decision_source='Explicit user confirmation of E2 proposal',
                           user_preference='Confirmed E2, including proposal selection and final-refit rules')
pending.update(status='CONFIRMED', confirmed_items=list(pending['items']), pending_items=[],
               e2_confirmed_at_sgt=datetime.now(timezone(timedelta(hours=8))).isoformat(),
               latest_user_statement='确认目前方案，采用E2，且确认A成员的02完成后开始03')
project['selected']['E'] = 'E2'
project['status'] = 'RULES_CONFIRMED'
project['training'].update(time_protocol='E2', proposal='confirmed: docs/E2滚动验证实验方案.md',
    rolling_folds=[{'train_years':[2017,2020], 'validation_year':2021},
                   {'train_years':[2017,2021], 'validation_year':2022},
                   {'train_years':[2017,2022], 'validation_year':2023}],
    selection_metric='Brier: equal-weight mean across frozen thresholds, then across three validation years',
    threshold_support_training_years=[2017,2020], final_refit_years=[2017,2023], final_test_year=2024,
    no_labels_cross_partition_boundary=True)
for path, data in [(pending_path,pending), (project_path,project)]:
    temporary=path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)
print('E2 confirmed; all 15 decisions recorded. Processing status unchanged.')
