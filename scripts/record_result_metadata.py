"""Write required data provenance alongside a real downstream result directory."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
from data_release import ROOT, provenance, sha256, verify_release, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--member', choices=['A', 'B', 'C'], required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--result-dir', type=Path, required=True)
    parser.add_argument('--experiment-config', type=Path, required=True)
    parser.add_argument('--command', required=True)
    parser.add_argument('--input-scope', choices=['sample', 'full'], required=True)
    args = parser.parse_args()
    result = args.result_dir.resolve()
    if not result.is_dir() or not args.experiment_config.is_file():
        raise RuntimeError('Actual result directory and experiment config must exist')
    output = result / 'result_metadata.json'
    if output.exists():
        raise RuntimeError('Metadata exists; create a new run directory')
    verification = verify_release(args.root, args.input_scope)
    files = [p for p in sorted(result.rglob('*')) if p.is_file()]
    if not files:
        raise RuntimeError('Result directory must contain actual outputs')
    write_json(output, {**provenance(args.root), 'member': args.member, 'run_id': args.run_id,
                       'created_at': datetime.now(timezone.utc).isoformat(),
                       'input_scope': args.input_scope, 'command': args.command,
                       'experiment_config_file': args.experiment_config.name,
                       'experiment_config_hash': sha256(args.experiment_config),
                       'input_integrity': verification,
                       'outputs': [{'path': p.relative_to(result).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in files]})
    print(f'Wrote {output}')


if __name__ == '__main__':
    main()
