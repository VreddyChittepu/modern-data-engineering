"""Static validation only. Does not emulate Spark or fabricate runtime test data."""
import argparse
import ast
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
ASSET='samples.nyctaxi.trips'


def validate():
    index=json.loads((ROOT/'course_index.json').read_text())
    paths=sorted((ROOT/'notebooks').glob('*.ipynb'))
    assert len(paths)==50==len(index)
    assert {str(p.relative_to(ROOT)) for p in paths}=={r['file'] for r in index}
    results=[]
    optional_switches={
        '00':'BROWSE_SAMPLE_VOLUME','24':'RESTORE_COURSE_TABLE','38':'RUN_CACHE_LAB',
        '39':'ENABLE_LIQUID_CLUSTERING','41':'PUBLISH_JOB_VALUES','42':'RUN_JDBC',
        '43':'RUN_DB2','44':'RUN_PYTEST','49':'RUN_CLASSIC_RDD'}
    total_code_cells=0
    for path in paths:
        notebook=json.loads(path.read_text())
        assert notebook['nbformat']==4 and notebook['nbformat_minor']==5
        assert notebook['metadata']['course']['source_asset']==ASSET
        cells=notebook['cells']
        assert len({c['id'] for c in cells})==len(cells)
        assert all(c['metadata']=={} for c in cells)
        codes=[c for c in cells if c['cell_type']=='code']
        assert len(codes)>=3
        assert all(c['outputs']==[] and c['execution_count'] is None for c in codes)
        trees=[]
        for i,cell in enumerate(codes):
            source=''.join(cell['source'])
            trees.append(ast.parse(source,filename=f'{path.name}:code-{i}'))
        all_nodes=[node for tree in trees for node in ast.walk(tree)]
        assignments={}
        for node in all_nodes:
            if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
                for target in node.targets:
                    if isinstance(target,ast.Name): assignments[target.id]=node.value.value
        assert assignments['SOURCE_ASSET']==ASSET
        number=path.name[:2]
        if number in optional_switches:
            assert assignments[optional_switches[number]] is False
        forbidden_roots={'pandas','numpy','sklearn','random','requests','urllib'}
        for node in all_nodes:
            if isinstance(node,ast.Import):
                assert all(n.name.split('.')[0] not in forbidden_roots for n in node.names)
            if isinstance(node,ast.ImportFrom):
                assert (node.module or '').split('.')[0] not in forbidden_roots
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                assert node.func.attr!='parallelize', 'No synthetic SparkContext fixtures.'
                if node.func.attr=='range':
                    assert not (isinstance(node.func.value,ast.Name) and node.func.value.id=='spark')
                if node.func.attr=='createDataFrame':
                    assert number=='43', 'Only derived DB2 outcome records need createDataFrame.'
                    assert isinstance(node.args[0],ast.Name) and node.args[0].id=='outcomes'
        # Enforce an explicit runtime assertion in every notebook and no unsafe cleanup.
        assert any(isinstance(n,ast.Assert) for n in all_nodes)
        text='\n'.join(''.join(c['source']) for c in codes)
        for forbidden in ('VACUUM RETAIN 0','retentionDurationCheck.enabled','DROP SCHEMA','https://raw.githubusercontent'):
            assert forbidden.lower() not in text.lower()
        if notebook['metadata']['course']['writes']:
            assert "target_catalog!='samples'" in text
            assert 'pyspark_lab_' in text
        total_code_cells+=len(codes)
        results.append({'notebook':path.name,'code_cells':len(codes),'structure':'passed','syntax':'passed',
                        'asset_provenance_lint':'passed','runtime_execution':'not performed'})
    # The separate pytest file must also compile; its Spark tests are not run locally.
    ast.parse((ROOT/'tests'/'test_asset_transformations.py').read_text())
    report={'notebooks':len(results),'code_cells':total_code_cells,'failures':0,
            'source_asset':ASSET,'validation_kind':'static; no Spark/Databricks runtime execution',
            'notebook_results':results}
    (ROOT/'docs'/'validation_results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(f'PASS: {len(results)} notebooks, {total_code_cells} Python cells; structure, syntax and asset-only lint.')
    print('NOT EXECUTED: Databricks asset reads, transformations, table/volume writes, streams, ML and external integrations.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    validate()
