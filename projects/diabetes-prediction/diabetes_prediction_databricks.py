# Databricks notebook source
# MAGIC %md
# MAGIC # Diabetes label prediction · Databricks + MLflow
# MAGIC 
# MAGIC Build a binary classifier: learn from seven measured attributes and predict the **historical Yes/No label** in the MASS Pima dataset. This notebook is a learning exercise, not a clinical diagnostic or future-risk model. Its historical cohort is restricted to adult women of Pima heritage near Phoenix; results do not establish accuracy for men, children, India or the general population.
# MAGIC 
# MAGIC ## Learning plan
# MAGIC 
# MAGIC 1. Check Databricks dataset assets and their schema.
# MAGIC 2. Clean missing values and keep the test set separate.
# MAGIC 3. Compare a baseline, logistic regression and random forest.
# MAGIC 4. Choose a model using training cross-validation and a threshold using validation data.
# MAGIC 5. Evaluate once on the official test split.
# MAGIC 6. Track/package the exact thresholded model in MLflow and reload it.
# MAGIC 
# MAGIC **Data rule:** read only `Pima.tr.csv` and `Pima.te.csv` from a Databricks sample asset location. No Kaggle, GitHub CSV, sklearn dataset, generated patients or external download fallback is used. Availability of these individual files in your workspace is unverified until the preflight cell passes.
# MAGIC 
# MAGIC **Setup:** attach Databricks Python compute. Add `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4`, and `mlflow>=2.20,<4` in the notebook Environment panel. Apply/restart and run top to bottom. Keep a compatible existing environment where possible; do not install a separate PySpark. The code uses NumPy and Spark directly, without pandas imports/conversions.
# MAGIC 
# MAGIC The official MASS split has 200 training records and 332 test records. We reserve 40 of the training records for validation and keep all 332 official test records locked until model/threshold selection. Pima.tr2 overlaps Pima.tr and must not be appended as extra independent training data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configuration and package versions
# MAGIC 
# MAGIC Keep the seed, source location and feature order explicit. The default directory is a candidate legacy Databricks Rdatasets asset location, not a guarantee that it exists on your compute.

# COMMAND ----------

import json, hashlib
from pathlib import PurePosixPath
from importlib.metadata import version
import numpy as np
from pyspark.sql import functions as F

SEED=42
FEATURES=['npreg','glu','bp','skin','bmi','ped','age']
EXPECTED_COUNTS={'Pima.tr.csv':200,'Pima.te.csv':332}
MAX_TOTAL_ROWS=600
TRACK_WITH_MLFLOW=True
REGISTER_MODEL=False
SAVE_PREDICTIONS=False
dbutils.widgets.text('asset_root','/databricks-datasets/Rdatasets/data-001/csv/MASS')
asset_root=dbutils.widgets.get('asset_root').strip()
versions={name:version(name) for name in ['numpy','scipy','scikit-learn','matplotlib','mlflow']}
print('Versions:',versions)
print('Feature order:',FEATURES)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Versions are printed. Missing packages should be configured before continuing; no installer or data downloader runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Preflight: require the actual Databricks asset
# MAGIC 
# MAGIC Validate the source boundary and list the directory. If the exact files are not available, stop. You may browse `/Volumes/samples/databricks/datasets/` in your workspace and set asset_root only if that official volume actually contains the same files. No path is guessed for its internal layout.

# COMMAND ----------

def validate_asset_root(value):
    if not isinstance(value,str): raise ValueError('asset_root must be text')
    canonical=value.removeprefix('dbfs:').rstrip('/')
    allowed=('/databricks-datasets/','/Volumes/samples/databricks/datasets/')
    if not any(canonical.startswith(prefix) for prefix in allowed):
        raise ValueError('Only Databricks sample asset paths are allowed; no external/custom dataset fallback.')
    if '..' in PurePosixPath(canonical).parts or '://' in canonical or '\\' in canonical:
        raise ValueError('Noncanonical source path rejected')
    return canonical

asset_root=validate_asset_root(asset_root)
try:
    files={entry.name.rstrip('/'):entry.path for entry in dbutils.fs.ls(asset_root)}
except Exception as exc:
    raise RuntimeError('Cannot list the Databricks sample asset directory. Confirm Pima.tr.csv and Pima.te.csv '
        'exist in an accessible Databricks-provided asset location. Training stops; no replacement data is loaded.') from exc
missing=set(EXPECTED_COUNTS)-set(files)
if missing:
    raise FileNotFoundError(f'Required Databricks asset files missing: {sorted(missing)}. Training cannot proceed.')
paths={name:asset_root+'/'+name for name in EXPECTED_COUNTS}
print('Asset preflight passed for:',list(paths))

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Both exact filenames must exist before any model training. A preflight failure is a real missing prerequisite, not a successful model run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Read and enforce the label/feature contract
# MAGIC 
# MAGIC Read the CSV columns as strings, then parse deliberately. Ignore a CSV row-name/index column if present; it must never be a feature. Invalid labels or malformed numbers fail visibly rather than being silently relabeled.

# COMMAND ----------

def read_asset(path,expected_rows):
    raw=(spark.read.option('header','true').option('inferSchema','false').option('mode','FAILFAST').csv(path))
    lower=[name.strip().lower() for name in raw.columns]
    if len(lower)!=len(set(lower)): raise ValueError('Duplicate normalized column names')
    raw=raw.toDF(*lower)
    required=set(FEATURES+['type'])
    if not required.issubset(raw.columns):
        raise ValueError(f'Unexpected asset schema; missing {sorted(required-set(raw.columns))}')
    label_text=F.lower(F.trim(F.col('type')))
    if raw.filter(label_text.isNull()|(~label_text.isin('yes','no'))).limit(1).count():
        raise ValueError('The target must contain only genuine Yes/No labels')
    for name in FEATURES:
        text=F.lower(F.trim(F.col(name)))
        missing=text.isNull()|text.isin('','na','nan','null')
        malformed=raw.filter((~missing)&F.expr(f'try_cast(`{name}` AS DOUBLE)').isNull())
        if malformed.limit(1).count(): raise ValueError(f'Malformed numeric source field: {name}')
    clean=raw.select(*[F.expr(f'try_cast(`{name}` AS DOUBLE)').alias(name) for name in FEATURES],
        F.when(label_text=='yes',1).otherwise(0).cast('int').alias('label'))
    rows=clean.orderBy(*FEATURES,'label').limit(MAX_TOTAL_ROWS+1).collect() # Bounded, deterministic ordering.
    if len(rows)!=expected_rows: raise ValueError(f'Expected {expected_rows} source records, got {len(rows)}; review the asset/version.')
    X=np.array([[row[name] if row[name] is not None else np.nan for name in FEATURES] for row in rows],dtype=np.float64)
    y=np.array([row['label'] for row in rows],dtype=np.int64)
    if np.isinf(X).any(): raise ValueError('Infinite measurements in source')
    if not np.array_equal(np.unique(y),[0,1]): raise ValueError('Both target classes are required')
    return X,y

X_development,y_development=read_asset(paths['Pima.tr.csv'],EXPECTED_COUNTS['Pima.tr.csv'])
# Load the test arrays, but do not inspect their labels or scores during model selection.
X_test,y_test=read_asset(paths['Pima.te.csv'],EXPECTED_COUNTS['Pima.te.csv'])
assert len(X_development)+len(X_test)<=MAX_TOTAL_ROWS
print('Development shape:',X_development.shape,'locked test shape:',X_test.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The feature matrices have shapes (200, 7) and (332, 7) when the documented asset version is present.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Normalize missing measurements and record provenance
# MAGIC 
# MAGIC A zero pregnancy count is valid. Zero values for glucose, blood pressure, skin thickness or BMI are treated as missing measurements in this educational preprocessing policy. Imputation statistics are learned later inside each training fold. Negative measurements fail rather than being silently corrected.

# COMMAND ----------

def clean_measurements(X):
    result=np.array(X,dtype=np.float64,copy=True)
    if result.ndim!=2 or result.shape[1]!=len(FEATURES): raise ValueError('Expected exactly seven ordered features')
    if np.isinf(result).any() or (result<0).any(): raise ValueError('Infinite or negative source measurements')
    for name in ['glu','bp','skin','bmi']:
        index=FEATURES.index(name)
        result[result[:,index]==0,index]=np.nan
    age=result[:,FEATURES.index('age')]
    if np.any(np.isfinite(age)&(age<21)): raise ValueError('Asset does not match the documented adult cohort')
    return result

X_development=clean_measurements(X_development)
X_test=clean_measurements(X_test)
def fingerprint(X,y):
    # Numeric parsed-content identity, not a clinical/patient identifier or original-file checksum.
    normalized=np.where(np.isnan(X),-1.0,X).astype('<f8').tobytes()+y.astype('<i8').tobytes()
    return hashlib.sha256(normalized).hexdigest()
provenance={
    'training_asset':paths['Pima.tr.csv'],'test_asset':paths['Pima.te.csv'],
    'training_parsed_sha256':fingerprint(X_development,y_development),
    'test_parsed_sha256':fingerprint(X_test,y_test),'feature_order':FEATURES,
    'zero_as_missing':['glu','bp','skin','bmi'],'target':{'No':0,'Yes':1}}
print('Development missing counts:',dict(zip(FEATURES,np.isnan(X_development).sum(axis=0).tolist())))
print('Training parsed fingerprint:',provenance['training_parsed_sha256'])

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Zero pregnancies remain zero; measurement gaps stay NaN until the training pipeline imputes them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Split development data; keep the official test set locked
# MAGIC 
# MAGIC Stratification preserves similar class proportions. Validation chooses the teaching decision threshold; the official test split is used only once after selection. Remove exact feature-duplicate contamination between development and test if found only after investigating provenance; do not silently clean it away.

# COMMAND ----------

from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV
X_fit,X_validation,y_fit,y_validation=train_test_split(
    X_development,y_development,test_size=0.20,stratify=y_development,random_state=SEED)
assert len(y_fit)==160 and len(y_validation)==40
assert min(np.bincount(y_fit))>=5
def feature_keys(X):
    return {tuple(np.where(np.isnan(row),-1.0,row)) for row in X}
overlap=feature_keys(X_development)&feature_keys(X_test)
if overlap:
    raise ValueError('Exact development/test feature overlap found. Investigate duplicate/subject identity before evaluation.')
if len(feature_keys(X_development))!=len(X_development):
    raise ValueError('Repeated development feature rows could cross internal folds; investigate subject identity before training.')
print('Fit/validation/test rows:',len(y_fit),len(y_validation),len(y_test))
print('Fit label counts:',np.bincount(y_fit).tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** 160 fit rows, 40 validation rows, and 332 locked test rows. Feature fingerprints cannot prove different people; subject-level identity is unavailable in this export.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Establish a simple baseline
# MAGIC 
# MAGIC A prior baseline gives every row the positive-class frequency learned on fit data. It has no individualized prediction skill. Compare the learned model against it instead of judging accuracy alone.

# COMMAND ----------

from sklearn.dummy import DummyClassifier
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
baseline=DummyClassifier(strategy='prior').fit(X_fit,y_fit)
print('Baseline positive-class probability:',baseline.class_prior_[1])

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The baseline learns only the fit-set class balance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Compare logistic regression and random forest
# MAGIC 
# MAGIC Logistic regression learns a weighted relationship between features and the label. A random forest combines decision trees. Both pipelines fit median imputation inside each cross-validation fold, so validation-fold values cannot influence the imputer. Scaling is applied only to the linear model.

# COMMAND ----------

pipeline=Pipeline([('imputer',SimpleImputer(strategy='median',keep_empty_features=True)),
    ('scale',StandardScaler()),('classifier',LogisticRegression(max_iter=2000,random_state=SEED))])
grid=[
    {'scale':[StandardScaler()],
     'classifier':[LogisticRegression(max_iter=2000,random_state=SEED)],
     'classifier__C':[0.1,1.0,10.0],'classifier__class_weight':[None,'balanced']},
    {'scale':['passthrough'],
     'classifier':[RandomForestClassifier(n_estimators=100,random_state=SEED,n_jobs=1)],
     'classifier__max_depth':[4,None],'classifier__min_samples_leaf':[3,6]}
]
cv=StratifiedKFold(n_splits=5,shuffle=True,random_state=SEED)
search=GridSearchCV(pipeline,grid,scoring={'roc_auc':'roc_auc','average_precision':'average_precision'},
    refit='roc_auc',cv=cv,n_jobs=1,error_score='raise',return_train_score=True)
search.fit(X_fit,y_fit)
selected_model=search.best_estimator_
print('Selected class:',type(selected_model.named_steps['classifier']).__name__)
print('Best fit-only CV ROC-AUC:',float(search.best_score_))
print('Selected parameters:',search.best_params_)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Ten candidate configurations are compared using fit data only; no metric value is fabricated or guaranteed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Review training cross-validation results
# MAGIC 
# MAGIC ROC-AUC measures ranking. Average precision summarizes precision/recall ranking and depends on prevalence. Fold variation is useful context; it is not a calibrated confidence interval.

# COMMAND ----------

comparison=[]
for i,params in enumerate(search.cv_results_['params']):
    comparison.append({'classifier':type(params['classifier']).__name__,
        'parameters':{k:str(v) for k,v in params.items() if k.startswith('classifier__')},
        'mean_cv_roc_auc':float(search.cv_results_['mean_test_roc_auc'][i]),
        'sd_cv_roc_auc':float(search.cv_results_['std_test_roc_auc'][i]),
        'mean_cv_average_precision':float(search.cv_results_['mean_test_average_precision'][i])})
for row in sorted(comparison,key=lambda r:-r['mean_cv_roc_auc']): print(row)
assert len(comparison)==10

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The table includes candidate configuration, mean scores and fold variability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Choose a threshold using validation only
# MAGIC 
# MAGIC A ranking model needs a cutoff to produce a label. We maximize validation F1 as a simple learning rule; this is not a clinically approved cutoff. The 40-row validation set makes the choice unstable. Do not refit after this step: keeping the same fitted model preserves the probability/threshold relationship we validated.

# COMMAND ----------

from sklearn.metrics import (roc_auc_score,average_precision_score,precision_score,recall_score,
    f1_score,accuracy_score,confusion_matrix,brier_score_loss)
validation_probability=selected_model.predict_proba(X_validation)[:,1]
thresholds=np.linspace(0.05,0.95,91)
validation_f1=np.array([f1_score(y_validation,validation_probability>=t,zero_division=0) for t in thresholds])
best_f1=float(validation_f1.max())
ties=np.flatnonzero(np.isclose(validation_f1,best_f1))
# Fixed tie-break: choose the tied threshold closest to 0.5, then the lower one.
chosen_index=min(ties,key=lambda i:(abs(float(thresholds[i])-0.5),float(thresholds[i])))
threshold=float(thresholds[chosen_index])
print('Validation threshold:',threshold,'validation F1:',best_f1)
assert 0<threshold<1

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The selected threshold comes only from validation labels. It is locked before evaluating the official test split.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Evaluate once on the official test split
# MAGIC 
# MAGIC Recall measures how many Yes labels are found. Precision measures how many predicted Yes labels are correct. The confusion matrix also reveals false positives and false negatives. These are dataset classification results, not evidence of general clinical validity.

# COMMAND ----------

def metric_report(labels,probability,cutoff):
    prediction=probability>=cutoff
    tn,fp,fn,tp=confusion_matrix(labels,prediction,labels=[0,1]).ravel()
    return {'roc_auc':float(roc_auc_score(labels,probability)),
        'average_precision':float(average_precision_score(labels,probability)),
        'accuracy':float(accuracy_score(labels,prediction)),
        'precision':float(precision_score(labels,prediction,zero_division=0)),
        'recall':float(recall_score(labels,prediction,zero_division=0)),
        'specificity':float(tn/(tn+fp)) if tn+fp else 0.0,
        'f1':float(f1_score(labels,prediction,zero_division=0)),
        'brier':float(brier_score_loss(labels,probability)),
        'tn':int(tn),'fp':int(fp),'fn':int(fn),'tp':int(tp)}

test_probability=selected_model.predict_proba(X_test)[:,1]
test_metrics=metric_report(y_test,test_probability,threshold)
baseline_metrics=metric_report(y_test,baseline.predict_proba(X_test)[:,1],0.5)
print('Selected model test metrics:',test_metrics)
print('Prior baseline test metrics:',baseline_metrics)
assert test_metrics['tn']+test_metrics['fp']+test_metrics['fn']+test_metrics['tp']==len(y_test)
assert np.isfinite(test_probability).all() and ((0<=test_probability)&(test_probability<=1)).all()
print('Do not change the model/threshold based on these test results and then reuse this test as untouched.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Report actual scores relative to the baseline; there is no forced minimum accuracy assertion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 — Plot ranking, errors and probability calibration
# MAGIC 
# MAGIC The ROC and precision-recall plots show ranking across cutoffs. The confusion matrix uses the chosen cutoff. Calibration compares predicted probabilities to observed label frequencies; it is an inspection only, not test-set calibration fitting.

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import RocCurveDisplay,PrecisionRecallDisplay,ConfusionMatrixDisplay
from sklearn.calibration import CalibrationDisplay
fig,axes=plt.subplots(2,2,figsize=(11,9))
RocCurveDisplay.from_predictions(y_test,test_probability,ax=axes[0,0],name='Selected model')
PrecisionRecallDisplay.from_predictions(y_test,test_probability,ax=axes[0,1],name='Selected model')
ConfusionMatrixDisplay.from_predictions(y_test,test_probability>=threshold,labels=[0,1],
    display_labels=['No','Yes'],ax=axes[1,0],colorbar=False)
CalibrationDisplay.from_predictions(y_test,test_probability,n_bins=5,strategy='quantile',ax=axes[1,1])
fig.suptitle('Historical Pima test split — educational evaluation')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Plots are generated from the actual held-out asset labels after runtime training; no placeholder curves are stored.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 — Package preprocessing and the chosen threshold together
# MAGIC 
# MAGIC A plain sklearn.predict() would normally use the classifier’s default decision rule. Package our chosen threshold explicitly so deployed/batch predictions agree with the evaluated notebook. Return two columns: model probability and predicted historical label. No personal patient inputs are requested.

# COMMAND ----------

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature

class ThresholdedDiabetesModel(mlflow.pyfunc.PythonModel):
    def __init__(self,model,cutoff):
        self.model=model
        self.cutoff=float(cutoff)
    def predict(self,context,model_input,params=None):
        values=clean_measurements(np.asarray(model_input,dtype=np.float64))
        if len(values)==0: return np.empty((0,2),dtype=np.float64)
        probability=self.model.predict_proba(values)[:,1]
        return np.column_stack([probability,(probability>=self.cutoff).astype(np.float64)])

packaged=ThresholdedDiabetesModel(selected_model,threshold)
example=X_validation[:3]
example_output=packaged.predict(None,example)
assert example_output.shape==(3,2)
assert np.array_equal(packaged.predict(None,X_test)[:,1],(test_probability>=threshold).astype(float))
print('Output contract: [model_probability_of_Yes, predicted_label_0_or_1]')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The packaged label exactly matches the thresholded prediction evaluated above. Probability estimates are not certified medical risk scores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 — Log lineage, results and the signed model in MLflow
# MAGIC 
# MAGIC The run records parsed-data fingerprints, feature order, missing-value policy, CV summaries, split sizes, the locked threshold, exact package versions, metrics and a model signature. The input example contains only historical public asset records.

# COMMAND ----------

if TRACK_WITH_MLFLOW:
    user=spark.sql('SELECT current_user()').first()[0]
    mlflow.set_tracking_uri('databricks')
    mlflow.set_experiment(f'/Users/{user}/diabetes_prediction_learning')
    with mlflow.start_run(run_name='pima_historical_label_classifier') as run:
        mlflow.log_params({'seed':SEED,'fit_rows':len(y_fit),'validation_rows':len(y_validation),
            'test_rows':len(y_test),'threshold':threshold,'selection_metric':'fit_cv_roc_auc',
            'classifier':type(selected_model.named_steps['classifier']).__name__,
            'cv_folds':5,'refit_after_threshold_selection':False})
        mlflow.log_metrics({'fit_cv_best_roc_auc':float(search.best_score_),
            'validation_f1_at_selected_threshold':best_f1,
            **{'test_'+k:v for k,v in test_metrics.items()},
            **{'baseline_test_'+k:v for k,v in baseline_metrics.items()}})
        mlflow.log_dict(provenance,'data_provenance.json')
        mlflow.log_dict({'versions':versions},'environment.json')
        mlflow.log_dict({'candidates':comparison},'training_cv_comparison.json')
        mlflow.log_dict({'input_features':FEATURES,'output_columns':['model_probability_of_Yes','predicted_historical_label'],
            'population':'Historical adult women of Pima heritage; not validated for general clinical use',
            'intended_use':'ML education only'},'model_contract.json')
        mlflow.log_figure(fig,'held_out_evaluation.png')
        info=mlflow.pyfunc.log_model(artifact_path='thresholded_classifier',python_model=packaged,
            signature=infer_signature(example,example_output),input_example=example,
            pip_requirements=[f'{name}=={versions[name]}' for name in ['numpy','scipy','scikit-learn','mlflow']])
        run_id=run.info.run_id
        model_uri=info.model_uri
    print('MLflow run:',run_id,'model:',model_uri)
else:
    run_id=None; model_uri=None
    print('MLflow tracking explicitly disabled; no tracking run or saved model is claimed.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** With tracking enabled, the experiment contains actual metrics and a reloadable threshold-aware artifact. Permissions/package errors remain visible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 — Reload and score only held-out asset records
# MAGIC 
# MAGIC Artifact round-trip verification is separate from model-quality evaluation. Batch examples are five real held-out records; no fabricated patient profile is introduced.

# COMMAND ----------

if TRACK_WITH_MLFLOW:
    loaded=mlflow.pyfunc.load_model(model_uri)
    scored=loaded.predict(X_test[:5])
    assert np.allclose(scored,packaged.predict(None,X_test[:5]))
    print('Artifact round trip passed')
else:
    scored=packaged.predict(None,X_test[:5])
for position,(probability,prediction) in enumerate(scored):
    print({'test_row_position':position,'model_probability_of_Yes':float(probability),
        'predicted_historical_label':int(prediction)})

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Reloaded and notebook predictions agree. Displayed row positions are export positions, not patient IDs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15 — Optional governed registration and prediction-table output
# MAGIC 
# MAGIC These switches are off by default. Register only as a clearly named educational model. Writing a course table does not deploy a medical service. Catalog/schema privileges and MLflow tracking are required.

# COMMAND ----------

dbutils.widgets.text('target_catalog','')
if REGISTER_MODEL or SAVE_PREDICTIONS:
    if not TRACK_WITH_MLFLOW: raise ValueError('Enable MLflow tracking first')
    target_catalog=dbutils.widgets.get('target_catalog').strip() or spark.sql('SELECT current_catalog()').first()[0]
    if target_catalog=='samples': raise ValueError('Choose a writable catalog, not samples')
    schema='diabetes_learning_'+hashlib.sha256(user.encode()).hexdigest()[:10]
    def quote(value): return '`'+value.replace('`','``')+'`'
    namespace=f'{quote(target_catalog)}.{quote(schema)}'
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
    if REGISTER_MODEL:
        mlflow.set_registry_uri('databricks-uc')
        registered=mlflow.register_model(model_uri,f'{target_catalog}.{schema}.pima_educational_classifier')
        print('Registered version:',registered.version)
    if SAVE_PREDICTIONS:
        all_scores=loaded.predict(X_test)
        records=[(int(i),float(row[0]),int(row[1]),int(y_test[i]),run_id) for i,row in enumerate(all_scores)]
        predictions=spark.createDataFrame(records,
            'test_row_position long, model_probability_of_yes double, predicted_historical_label int, actual_historical_label int, run_id string')
        output=f'{namespace}.`pima_test_predictions`'
        predictions.withColumn('scored_at',F.current_timestamp()).write.format('delta').mode('overwrite').saveAsTable(output)
        assert spark.table(output).count()==len(y_test)
        print('Course prediction table:',output)
else:
    print('Optional registry and Delta output skipped; no serving endpoint is created.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Default mode logs the MLflow artifact but does not register a model, create a serving endpoint or write prediction tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice and solution guidance
# MAGIC 
# MAGIC 1. **Explain the label.** Is the model predicting a future diagnosis? **Guide:** no; it reproduces a historical binary label. The export provides no forecasting horizon or future outcome timestamps.
# MAGIC 2. **Change the model selection metric before rerunning a fresh experiment.** Compare ROC-AUC with average precision. **Guide:** change `refit` in GridSearchCV, use development/validation only, and treat repeated official-test review as exploratory rather than untouched evaluation.
# MAGIC 3. **Explore recall/precision tradeoffs on validation.** **Guide:** compute both metrics for candidate thresholds without selecting on test labels. A medically useful operating point requires clinical objectives and external evidence, not this classroom F1 rule.
# MAGIC 4. **Inspect probability reliability.** **Guide:** Brier score and the calibration plot evaluate probability agreement. If fitting a calibrator, use a training-only nested/calibration split; do not fit it on the official test set.
# MAGIC 5. **Track the run in Databricks MLflow UI.** **Guide:** open the printed experiment/run, compare candidate summaries, inspect the source paths and package versions, and verify the round-trip assertion.
# MAGIC 
# MAGIC ## Limitations to explain in your portfolio
# MAGIC 
# MAGIC The dataset is small, historical and restricted to a specific cohort. It cannot establish performance in your local population. Measurements including glucose may be closely related to how the original label was determined; a high score does not prove early screening utility. There is no treatment recommendation, personal diagnosis, clinical deployment or guaranteed accuracy. Approval for real clinical use would require representative external validation, appropriate prospective evaluation, calibration, oversight and clinical governance.
# MAGIC 
# MAGIC If the source preflight fails, stop there: the project remains blocked on a Databricks-provided asset with these genuine labels. Do not replace it with taxi data, `sklearn.load_diabetes` (a different regression task), a web download or generated patient records.
