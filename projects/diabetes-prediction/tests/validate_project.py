"""Local structural/source-policy checks; no clinical data or Spark execution."""
import ast
import json
from pathlib import Path, PurePosixPath

ROOT=Path(__file__).resolve().parents[1]


def validate():
    notebook=json.loads((ROOT/'diabetes_prediction_databricks.ipynb').read_text())
    assert notebook['nbformat']==4
    cells=notebook['cells']
    assert len({c['id'] for c in cells})==len(cells)
    code_cells=[c for c in cells if c['cell_type']=='code']
    assert len(code_cells)==15
    sources=[''.join(c['source']) for c in code_cells]
    assert all(c['execution_count'] is None and c['outputs']==[] for c in code_cells)
    trees=[ast.parse(s) for s in sources]
    code='\n\n'.join(sources)
    export=(ROOT/'diabetes_prediction_databricks.py').read_text()
    assert ast.dump(ast.parse(export),include_attributes=False)==ast.dump(ast.parse(code),include_attributes=False)
    nodes=[node for tree in trees for node in ast.walk(tree)]
    imports=[]
    for node in nodes:
        if isinstance(node,ast.Import):imports.extend(alias.name for alias in node.names)
        if isinstance(node,ast.ImportFrom):imports.append(node.module or '')
    assert not any(m.startswith(('pandas','sklearn.datasets','requests','urllib')) for m in imports)
    assert 'read_csv' not in code and 'load_diabetes' not in code
    assert "'Pima.tr.csv'" in code and "'Pima.te.csv'" in code
    assignments={}
    for node in nodes:
        if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
            for target in node.targets:
                if isinstance(target,ast.Name):assignments[target.id]=node.value.value
    assert assignments['TRACK_WITH_MLFLOW'] is True
    assert assignments['REGISTER_MODEL'] is False and assignments['SAVE_PREDICTIONS'] is False
    fits=[n for n in nodes if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='fit']
    assert len(fits)==2
    for node in fits:
        assert [arg.id for arg in node.args if isinstance(arg,ast.Name)]==['X_fit','y_fit']
    assert "search.best_estimator_" in code
    assert "validation_probability>=t" in code
    assert "test_probability=selected_model.predict_proba(X_test)" in code
    # Execute the actual notebook's source-boundary function, using only path strings.
    fn=next(n for n in nodes if isinstance(n,ast.FunctionDef) and n.name=='validate_asset_root')
    module=ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[]))
    scope={'PurePosixPath':PurePosixPath}
    exec(compile(module,'notebook_source_path_guard','exec'),scope)
    guard=scope['validate_asset_root']
    accepted=[('/databricks-datasets/Rdatasets/data-001/csv/MASS/', '/databricks-datasets/Rdatasets/data-001/csv/MASS'),
              ('dbfs:/databricks-datasets/Rdatasets/data-001/csv/MASS','/databricks-datasets/Rdatasets/data-001/csv/MASS'),
              ('/Volumes/samples/databricks/datasets/observed/MASS','/Volumes/samples/databricks/datasets/observed/MASS')]
    rejected=['https://example.com/Pima.tr.csv','/Volumes/my_catalog/other/data','/tmp/MASS',
              '/databricks-datasets/../outside','/Volumes/samples/databricks/datasets/../outside',None]
    for value,expected in accepted:assert guard(value)==expected
    for value in rejected:
        try:guard(value)
        except ValueError:pass
        else:raise AssertionError(f'Untrusted source path accepted: {value!r}')
    report={'notebook_code_cells':len(code_cells),'syntax':'passed','source_export_equivalence':'passed',
        'no_external_or_synthetic_dataset_imports':'passed','fit_input_static_check':'passed',
        'source_path_cases':len(accepted)+len(rejected),'source_path_checks':'passed',
        'databricks_asset_availability':'unverified','training_and_mlflow_runtime':'not executed',
        'clinical_performance':'not established','failures':0}
    (ROOT/'docs'/'validation_results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':validate()
