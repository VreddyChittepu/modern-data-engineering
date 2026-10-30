"""Local code/policy checks only; no fabricated financial transaction dataset."""
import ast
import json
from pathlib import Path, PurePosixPath
import numpy as np

ROOT=Path(__file__).resolve().parents[1]


def validate():
    notebook=json.loads((ROOT/'credit_card_fraud_databricks.ipynb').read_text())
    assert notebook['nbformat']==4
    cells=notebook['cells']
    assert len({c['id'] for c in cells})==len(cells)
    codes=[c for c in cells if c['cell_type']=='code']
    assert len(codes)==16
    assert all(c['outputs']==[] and c['execution_count'] is None for c in codes)
    sources=[''.join(c['source']) for c in codes]
    trees=[ast.parse(s) for s in sources]
    code='\n\n'.join(sources)
    export=(ROOT/'credit_card_fraud_databricks.py').read_text()
    assert ast.dump(ast.parse(code),include_attributes=False)==ast.dump(ast.parse(export),include_attributes=False)
    nodes=[n for tree in trees for n in ast.walk(tree)]
    forbidden=('pandas','sklearn.datasets','imblearn','requests','urllib')
    for node in nodes:
        if isinstance(node,ast.Import):assert not any(alias.name.startswith(forbidden) for alias in node.names)
        if isinstance(node,ast.ImportFrom):assert not (node.module or '').startswith(forbidden)
    assignments={}
    for node in nodes:
        if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
            for name in node.targets:
                if isinstance(name,ast.Name):assignments[name.id]=node.value.value
    assert assignments['TRACK_WITH_MLFLOW'] is True
    assert assignments['SAVE_PREDICTIONS'] is False and assignments['LOG_INPUT_EXAMPLE'] is False
    assert assignments['PCA_DIMENSIONS']==28 and assignments['MAX_ROWS']<=300000
    fits=[n for n in nodes if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='fit']
    assert len(fits)==2
    for call in fits:assert [a.id for a in call.args if isinstance(a,ast.Name)]==['X_fit','y_fit']
    assert 'train_test_split' not in code
    assert "vector_to_array('pcaVector')" in code
    assert 'times[fit_mask].max()<times[validation_mask].min()' in code
    assert 'time_fit[train_idx].max()<time_fit[val_idx].min()' in code
    assert "refit='average_precision'" in code
    assert "precision_recall_curve(y_validation,validation_probability)" in code
    assert 'test_probability=selected_model.predict_proba(X_test)' in code
    assert 'np.lexsort((test_keys,-test_probability))' in code
    # Exercise actual helpers on source strings/policy metadata, not transaction fixtures.
    helpers=[n for n in nodes if isinstance(n,ast.FunctionDef) and n.name in {'validate_source_path','validate_alert_budget'}]
    module=ast.fix_missing_locations(ast.Module(body=helpers,type_ignores=[]))
    scope={'PurePosixPath':PurePosixPath,'np':np}
    exec(compile(module,'notebook_policy_helpers','exec'),scope)
    path_guard=scope['validate_source_path'];budget_guard=scope['validate_alert_budget']
    accepted=[('/databricks-datasets/credit-card-fraud/data/','/databricks-datasets/credit-card-fraud/data'),
              ('dbfs:/databricks-datasets/credit-card-fraud/data','/databricks-datasets/credit-card-fraud/data'),
              ('/Volumes/samples/databricks/datasets/observed/fraud/data','/Volumes/samples/databricks/datasets/observed/fraud/data')]
    rejected=['https://example.com/creditcard.csv','/tmp/data','/Volumes/custom/fraud',
              '/databricks-datasets/../outside','/Volumes/samples/databricks/datasets/../outside',None]
    for value,expected in accepted:assert path_guard(value)==expected
    for value in rejected:
        try:path_guard(value)
        except ValueError:pass
        else:raise AssertionError('Untrusted source path accepted')
    for value in [0.01,0.05,0.5]:assert budget_guard(value)==value
    bad_budget=[0,1,-0.1,1.5,True,None,'0.01',float('nan'),float('inf')]
    for value in bad_budget:
        try:budget_guard(value)
        except ValueError:pass
        else:raise AssertionError('Invalid review budget accepted')
    report={'notebook_code_cells':len(codes),'syntax':'passed','source_export_equivalence':'passed',
        'asset_only_source_policy':'passed','source_path_cases':len(accepted)+len(rejected),
        'review_budget_cases':3+len(bad_budget),'fit_input_and_chronology_static_checks':'passed',
        'optional_output_disabled':'passed','databricks_asset_availability':'unverified',
        'training_mlflow_runtime':'not executed','financial_decisions':'not implemented','failures':0}
    (ROOT/'docs'/'validation_results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':validate()
