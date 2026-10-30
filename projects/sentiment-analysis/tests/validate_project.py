"""Check source policy and notebook contracts without a substitute review dataset."""
import ast
import json
from pathlib import Path, PurePosixPath

ROOT=Path(__file__).resolve().parents[1]


def validate():
    notebook=json.loads((ROOT/'sentiment_analysis_databricks.ipynb').read_text())
    assert notebook['nbformat']==4
    cells=notebook['cells']
    assert len({c['id'] for c in cells})==len(cells)
    codes=[c for c in cells if c['cell_type']=='code']
    assert len(codes)==16
    assert all(c['outputs']==[] and c['execution_count'] is None for c in codes)
    sources=[''.join(c['source']) for c in codes]
    trees=[ast.parse(s) for s in sources]
    combined='\n\n'.join(sources)
    export=(ROOT/'sentiment_analysis_databricks.py').read_text()
    assert ast.dump(ast.parse(combined),include_attributes=False)==ast.dump(ast.parse(export),include_attributes=False)
    nodes=[n for tree in trees for n in ast.walk(tree)]
    forbidden=('pandas','sklearn.datasets','requests','urllib','transformers','datasets')
    for node in nodes:
        if isinstance(node,ast.Import):assert not any(alias.name.startswith(forbidden) for alias in node.names)
        if isinstance(node,ast.ImportFrom):assert not (node.module or '').startswith(forbidden)
    assignments={}
    for node in nodes:
        if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
            for name in node.targets:
                if isinstance(name,ast.Name):assignments[name.id]=node.value.value
    assert assignments['TRACK_WITH_MLFLOW'] is True
    for flag in ['RUN_AI_FUNCTION','SAVE_PREDICTIONS','PRINT_REVIEW_TEXT','LOG_RAW_INPUT_EXAMPLE']:
        assert assignments[flag] is False
    fits=[n for n in nodes if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='fit']
    assert len(fits)==2
    for call in fits:assert [a.id for a in call.args if isinstance(a,ast.Name)]==['fit_texts','y_fit']
    assert "preprocessor=normalize_review" in combined and "stop_words=None" in combined
    assert "validation_probability>=t" in combined
    assert "test_probability=selected_model.predict_proba(test_texts)" in combined
    assert "test=[r for r in candidate_test if r['review_id'] not in development_ids]" in combined
    # Execute real notebook helpers on path strings and rating-policy metadata only.
    helper_names={'validate_asset_root','label_from_rating'}
    helpers=[n for n in nodes if isinstance(n,ast.FunctionDef) and n.name in helper_names]
    scope={'PurePosixPath':PurePosixPath}
    module=ast.fix_missing_locations(ast.Module(body=helpers,type_ignores=[]))
    exec(compile(module,'notebook_helpers','exec'),scope)
    guard=scope['validate_asset_root'];label=scope['label_from_rating']
    good=[('/databricks-datasets/amazon/','/databricks-datasets/amazon'),
          ('dbfs:/databricks-datasets/amazon','/databricks-datasets/amazon'),
          ('/Volumes/samples/databricks/datasets/observed/amazon','/Volumes/samples/databricks/datasets/observed/amazon')]
    bad=['https://example.com/reviews','/Volumes/custom/reviews','/tmp/reviews',
         '/databricks-datasets/../outside','/Volumes/samples/databricks/datasets/../outside',None]
    for value,expected in good:assert guard(value)==expected
    for value in bad:
        try:guard(value)
        except ValueError:pass
        else:raise AssertionError('Untrusted source accepted')
    for value,expected in [(1,0),(2,0),(3,None),(4,1),(5,1)]:assert label(value)==expected
    for value in [0,6,None,True,'5']:
        try:label(value)
        except ValueError:pass
        else:raise AssertionError('Invalid rating accepted')
    report={'notebook_code_cells':len(codes),'syntax':'passed','notebook_source_equivalence':'passed',
        'asset_only_source_policy':'passed','source_path_cases':len(good)+len(bad),
        'rating_policy_cases':10,'fit_input_static_checks':'passed','optional_calls_disabled':'passed',
        'databricks_asset_availability':'unverified','training_mlflow_ai_runtime':'not executed','failures':0}
    (ROOT/'docs'/'validation_results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':validate()
