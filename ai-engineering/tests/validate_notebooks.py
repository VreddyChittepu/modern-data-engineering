"""Validate structure/syntax, then execute portable and optional-default cells.

No Spark mocks or remote-service stubs: unavailable integrations stay unexecuted.
Run from any directory: python /path/to/ai-engineering/tests/validate_notebooks.py
"""
import ast
import contextlib
import io
import json
import os
from pathlib import Path
import time
import traceback

for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'):
    os.environ[key] = '1'

ROOT = Path(__file__).resolve().parents[1]


def main():
    report = {'syntax_checked': [], 'executed': [], 'databricks_not_executed': [], 'failures': []}
    paths = sorted((ROOT / 'notebooks').glob('*.ipynb'))
    index = json.loads((ROOT / 'course_index.json').read_text())
    assert len(paths) == 30 == len(index)
    assert {p.name for p in paths} == {Path(row['file']).name for row in index}
    for path in paths:
        started = time.perf_counter()
        try:
            nb = json.loads(path.read_text())
            assert nb['nbformat'] == 4
            mode = nb['metadata']['course']['execution_mode']
            assert mode in {'portable', 'databricks', 'optional'}
            namespace = {'__name__': '__main__'}
            assert len({cell['id'] for cell in nb['cells']}) == len(nb['cells'])
            code_cells = [(i, ''.join(c['source'])) for i, c in enumerate(nb['cells']) if c['cell_type'] == 'code']
            assert len(code_cells) >= 3
            for i, source in code_cells:
                ast.parse(source, filename=f'{path.name}:cell-{i}')
            report['syntax_checked'].append(path.name)
            if mode == 'databricks':
                report['databricks_not_executed'].append(path.name)
                print('SYNTAX ONLY', path.name, flush=True)
                continue
            with contextlib.redirect_stdout(io.StringIO()):
                for i, source in code_cells:
                    exec(compile(source, f'{path.name}:cell-{i}', 'exec'), namespace)
            report['executed'].append({'notebook': path.name, 'seconds': round(time.perf_counter()-started, 3), 'mode': mode})
            print('PASS', path.name, flush=True)
        except Exception:
            detail = traceback.format_exc()
            report['failures'].append({'notebook': path.name, 'traceback': detail})
            print('FAIL', path.name, detail, flush=True)
    report_path = ROOT / 'docs' / 'validation_results.json'
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(f"Syntax: {len(report['syntax_checked'])}/30; executed: {len(report['executed'])}; failures: {len(report['failures'])}")
    if report['failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
