# Databricks notebook source
# MAGIC %md
# MAGIC # Credit-card fraud detection · Databricks + MLflow
# MAGIC 
# MAGIC Learn a rare-event classifier from the **Databricks-provided credit-card-fraud sample asset**. Spark reads and validates the source; a bounded scikit-learn workflow trains on the driver. Only genuine source transactions and fraud labels are used: no external download, generated transaction, SMOTE dataset or pandas conversion.
# MAGIC 
# MAGIC **Default asset candidate:** `/databricks-datasets/credit-card-fraud/data` (Parquet). Its documented contract is `time`, `amountRange`, `label`, and `pcaVector`. Do not substitute a CSV with `V1`–`V28`/`Amount`/`Class` and claim it is the same source contract.
# MAGIC 
# MAGIC **Task:** `label=1` means historical fraud and `label=0` means historical non-fraud. A model flag means a candidate for review, not proof of fraud or an instruction to decline a payment. No banking integration or live transaction blocking is implemented.
# MAGIC 
# MAGIC **Setup:** configure compatible `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4`, and `mlflow>=2.20,<4` in the notebook Environment panel. Apply/restart and run top to bottom. Use Databricks-provided PySpark; vector_to_array requires supported Spark/compute APIs. Source availability must pass preflight in your own workspace.
# MAGIC 
# MAGIC You will profile imbalance, enforce data quality, keep chronological evaluation periods, compare weighted classifiers against a baseline, choose a validation review budget, inspect precision/recall and ranking, and log/reload a threshold-aware model in MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configure the learning limits and policy
# MAGIC 
# MAGIC Default to the earliest 100,000 valid unique source records. This is a chronological learning slice, not a random representative sample of all transactions. The maximum controls driver memory; scanning/sorting the source may still process more data.

# COMMAND ----------

import json, hashlib
from pathlib import PurePosixPath
from importlib.metadata import version
import numpy as np
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array

SEED=42
MAX_ROWS=100000
PCA_DIMENSIONS=28
FEATURE_ORDER=[f'pca_{i+1:02d}' for i in range(PCA_DIMENSIONS)]+['amount_range']
MAX_VALIDATION_ALERT_RATE=0.01 # Illustrative 1% review budget, not a production recommendation.
TRACK_WITH_MLFLOW=True
LOG_INPUT_EXAMPLE=False
SAVE_PREDICTIONS=False
assert 50000<=MAX_ROWS<=300000
def validate_alert_budget(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)) or not np.isfinite(value) or not 0<value<1:
        raise ValueError('Alert-rate budget must be finite and strictly between 0 and 1')
    return float(value)
MAX_VALIDATION_ALERT_RATE=validate_alert_budget(MAX_VALIDATION_ALERT_RATE)
dbutils.widgets.text('source_path','/databricks-datasets/credit-card-fraud/data')
source_path=dbutils.widgets.get('source_path').strip()
versions={name:version(name) for name in ['numpy','scipy','scikit-learn','matplotlib','mlflow']}
print('Versions:',versions,'row cap:',MAX_ROWS,'validation alert budget:',MAX_VALIDATION_ALERT_RATE)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** No source data, real money, endpoint or payment action is invented.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Preflight the actual asset and schema
# MAGIC 
# MAGIC Allow only Databricks-provided sample locations. If the same asset is exposed under `/Volumes/samples/databricks/datasets/`, set source_path to its observed location. No custom upload or external-data fallback is accepted.

# COMMAND ----------

def validate_source_path(value):
    if not isinstance(value,str):raise ValueError('Source path must be text')
    canonical=value.removeprefix('dbfs:').rstrip('/')
    prefixes=('/databricks-datasets/','/Volumes/samples/databricks/datasets/')
    if not any(canonical.startswith(prefix) for prefix in prefixes):
        raise ValueError('Only Databricks sample-asset locations are allowed')
    if '..' in PurePosixPath(canonical).parts or '://' in canonical or '\\' in canonical:
        raise ValueError('Noncanonical source path rejected')
    return canonical

source_path=validate_source_path(source_path)
try:
    source=spark.read.parquet(source_path)
    required={'time','amountRange','label','pcaVector'}
    if not required.issubset(source.columns):raise ValueError(f'Missing source fields: {sorted(required-set(source.columns))}')
    if source.limit(1).count()!=1:raise ValueError('Source asset is empty')
except Exception as exc:
    raise RuntimeError('Databricks fraud asset unavailable/incompatible. Resolve asset access and its documented schema; '
        'no substitute dataset is loaded.') from exc
source.printSchema()
print('Validated source:',source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The real source must provide its fraud label and PCA vector before training can start.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Validate labels and vector values
# MAGIC 
# MAGIC Convert the supplied PCA vector using the Spark-native helper. Negative PCA coordinates are valid; do not apply a generic nonnegative rule. amountRange is a category 0–7, not an exact transaction amount.

# COMMAND ----------

source_rows=source.count()
missing_vector_rows=source.filter(F.col('pcaVector').isNull()).count()
prepared=(source.filter(F.col('pcaVector').isNotNull())
    .select(F.expr('try_cast(time AS DOUBLE)').alias('event_time'),
        F.expr('try_cast(amountRange AS DOUBLE)').alias('amount_range'),
        F.expr('try_cast(label AS DOUBLE)').alias('label'),vector_to_array('pcaVector').alias('pca')))
bad_vector=F.exists('pca',lambda x:x.isNull()|F.isnan(x)|(F.abs(x)==F.lit(float('inf'))))
checked=prepared.withColumn('reason',
    F.when(F.col('event_time').isNull()|F.isnan('event_time')|(F.abs('event_time')==F.lit(float('inf')))|(F.col('event_time')<0),'invalid_time')
     .when(F.col('amount_range').isNull()|(~F.col('amount_range').isin(*range(8))),'invalid_amount_range')
     .when(F.col('label').isNull()|(~F.col('label').isin(0.0,1.0)),'invalid_label')
     .when((F.size('pca')!=PCA_DIMENSIONS)|bad_vector,'invalid_pca'))
invalid_rows=checked.filter('reason IS NOT NULL').count()
valid=checked.filter('reason IS NULL').drop('reason')
assert valid.count()+invalid_rows+missing_vector_rows==source_rows
print({'source_rows':source_rows,'missing_vector_rows':missing_vector_rows,'other_rejected_rows':invalid_rows})
checked.filter('reason IS NOT NULL').groupBy('reason').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Quality counts reconcile with the source. Valid negative PCA values remain unchanged.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Deduplicate exact content and inspect class imbalance
# MAGIC 
# MAGIC A fingerprint identifies identical supplied time/bucket/vector content, not a true bank transaction ID. Conflicting labels for identical content stop training. Identical content is collapsed before splitting so exact replays cannot cross a boundary.

# COMMAND ----------

valid=valid.withColumn('transaction_key',F.sha2(F.to_json(F.struct('event_time','amount_range','pca')),256))
conflicts=valid.groupBy('transaction_key').agg(F.countDistinct('label').alias('labels')).filter('labels > 1')
if conflicts.limit(1).count():raise ValueError('Conflicting labels for the same supplied transaction content; investigate source provenance')
unique=valid.dropDuplicates(['transaction_key'])
unique_rows=unique.count()
print('Exact duplicate occurrences removed:',valid.count()-unique_rows)
unique.groupBy('label').count().orderBy('label').show()
bounded=unique.orderBy('event_time','transaction_key').limit(MAX_ROWS)
rows=bounded.collect() # Explicitly bounded learning workflow; do not collect unbounded production data.
if len(rows)<1000:raise ValueError('Too few genuine valid transactions')
X=np.array([list(row['pca'])+[row['amount_range']] for row in rows],dtype=np.float64)
y=np.array([int(row['label']) for row in rows],dtype=np.int64)
times=np.array([row['event_time'] for row in rows],dtype=np.float64)
transaction_keys=np.array([row['transaction_key'] for row in rows],dtype=str)
assert X.shape==(len(rows),29) and np.isfinite(X).all()
print('Retained slice rows:',len(rows),'fraud count:',int(y.sum()),'fraud prevalence:',float(y.mean()))
print('A high all-legitimate accuracy can hide zero fraud recall; ranking and review errors matter.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Natural class proportions of the chosen chronological slice are retained; no resampling changes validation/test prevalence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Split earlier fit data from later validation/test data
# MAGIC 
# MAGIC Use time boundaries without splitting equal-time transactions across periods. Relative source time is not a calendar date. This retrospective two-period-style sample cannot prove deployment performance under future drift.

# COMMAND ----------

fit_end,validation_end=np.quantile(np.unique(times),[0.60,0.80],method='nearest')
fit_mask=times<=fit_end
validation_mask=(times>fit_end)&(times<=validation_end)
test_mask=times>validation_end
X_fit,y_fit,time_fit=X[fit_mask],y[fit_mask],times[fit_mask]
X_validation,y_validation=X[validation_mask],y[validation_mask]
X_test,y_test=X[test_mask],y[test_mask]
test_keys=transaction_keys[test_mask]
for name,labels in [('fit',y_fit),('validation',y_validation),('test',y_test)]:
    counts=np.bincount(labels,minlength=2)
    if len(labels)<100 or min(counts)<5:
        raise ValueError(f'{name} has insufficient real fraud/non-fraud records. Increase MAX_ROWS or review the time slice; no random fallback is used.')
    print(name,{'rows':len(labels),'non_fraud':int(counts[0]),'fraud':int(counts[1]),'prevalence':float(labels.mean())})
assert times[fit_mask].max()<times[validation_mask].min()
assert times[validation_mask].max()<times[test_mask].min()
assert not (set(transaction_keys[fit_mask])&set(test_keys))
print('Source-time boundaries:',float(fit_end),float(validation_end))

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The test interval occurs strictly after validation; no random shuffled split is substituted when minority counts are low.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Construct forward-only cross-validation folds
# MAGIC 
# MAGIC Expand training windows in source-time order. Split unique timestamps first, then map them to rows so the same second never crosses a fold boundary. Transaction arrival times are irregular, so folds may span different durations and class proportions.

# COMMAND ----------

from sklearn.model_selection import TimeSeriesSplit,GridSearchCV
unique_fit_times=np.unique(time_fit)
cv=[];fold_manifest=[]
for fold,(train_time_idx,validation_time_idx) in enumerate(TimeSeriesSplit(n_splits=3).split(unique_fit_times)):
    train_end=unique_fit_times[train_time_idx[-1]]
    val_end=unique_fit_times[validation_time_idx[-1]]
    train_idx=np.flatnonzero(time_fit<=train_end)
    val_idx=np.flatnonzero((time_fit>train_end)&(time_fit<=val_end))
    for indices in [train_idx,val_idx]:
        if min(np.bincount(y_fit[indices],minlength=2))<3:
            raise ValueError('A forward CV fold has too few fraud examples. Increase the genuine source slice; do not synthesize minority data for evaluation.')
    assert time_fit[train_idx].max()<time_fit[val_idx].min()
    cv.append((train_idx,val_idx))
    fold_manifest.append({'fold':fold,'train_rows':len(train_idx),'validation_rows':len(val_idx),
        'train_fraud':int(y_fit[train_idx].sum()),'validation_fraud':int(y_fit[val_idx].sum()),
        'train_last_source_time':float(train_end),'validation_last_source_time':float(val_end)})
print(fold_manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Every fold fits on earlier source times and validates on later times, with real examples of both classes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Define preprocessing and weighted models
# MAGIC 
# MAGIC Scale PCA coordinates inside each fold and one-hot encode the eight amount categories. The category universe is documented metadata. Class weights are computed from the training data only; validation and test remain untouched. Time is deliberately used only for splitting, not as a model feature.

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.dummy import DummyClassifier

preprocessor=ColumnTransformer([
    ('pca',StandardScaler(),list(range(28))),
    ('amount_bucket',OneHotEncoder(categories=[list(range(8))],handle_unknown='error',sparse_output=False),[28])])
pipeline=Pipeline([('preprocessor',preprocessor),('classifier',LogisticRegression(solver='liblinear',max_iter=2000,random_state=SEED))])
grid=[{'classifier':[LogisticRegression(class_weight='balanced',solver='liblinear',max_iter=2000,random_state=SEED)],
       'classifier__C':[0.1,1.0]},
      {'classifier':[RandomForestClassifier(n_estimators=80,max_depth=8,class_weight='balanced_subsample',random_state=SEED,n_jobs=1)],
       'classifier__min_samples_leaf':[5,20]}]
baseline=DummyClassifier(strategy='constant',constant=0).fit(X_fit,y_fit)
search=GridSearchCV(pipeline,grid,cv=cv,scoring={'average_precision':'average_precision','roc_auc':'roc_auc'},
    refit='average_precision',n_jobs=1,error_score='raise')
search.fit(X_fit,y_fit)
selected_model=search.best_estimator_
print('Selected classifier:',type(selected_model['classifier']).__name__)
print('Training CV average precision:',float(search.best_score_))

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Four configurations are selected using training-only average precision. Accuracy is not the selection metric.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Inspect candidate ranking quality
# MAGIC 
# MAGIC Average precision (AP) summarizes the precision–recall ranking. It is not numerically identical to every trapezoidal PR-AUC implementation. AP depends on fraud prevalence, so do not compare scores across different sampling policies as if they were equivalent.

# COMMAND ----------

comparison=[]
for i,params in enumerate(search.cv_results_['params']):
    comparison.append({'classifier':type(params['classifier']).__name__,
        'parameters':{k:str(v) for k,v in params.items() if k.startswith('classifier__')},
        'mean_cv_average_precision':float(search.cv_results_['mean_test_average_precision'][i]),
        'sd_cv_average_precision':float(search.cv_results_['std_test_average_precision'][i]),
        'mean_cv_roc_auc':float(search.cv_results_['mean_test_roc_auc'][i])})
for row in sorted(comparison,key=lambda r:-r['mean_cv_average_precision']):print(row)
assert len(comparison)==4

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Only real runtime scores are printed; no guaranteed accuracy or AP is asserted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Choose a validation-only review threshold
# MAGIC 
# MAGIC Among thresholds flagging at most the illustrative validation review budget, maximize fraud recall, then precision, then minimize alert count. Score ties are all included at the cutoff. The deployed/test alert rate can differ as the score distribution changes. No cost in dollars is inferred from amount buckets.

# COMMAND ----------

from sklearn.metrics import (precision_recall_curve,average_precision_score,roc_auc_score,
    precision_score,recall_score,f1_score,accuracy_score,confusion_matrix,brier_score_loss)
validation_probability=selected_model.predict_proba(X_validation)[:,1]
precision_values,recall_values,cutoffs=precision_recall_curve(y_validation,validation_probability)
alerts=len(y_validation)-np.searchsorted(np.sort(validation_probability),cutoffs,side='left')
eligible=np.flatnonzero((alerts>0)&(alerts/len(y_validation)<=MAX_VALIDATION_ALERT_RATE))
if len(eligible):
    best=max(eligible,key=lambda i:(float(recall_values[i]),float(precision_values[i]),-int(alerts[i]),float(cutoffs[i])))
    threshold=float(cutoffs[best])
    threshold_status='nonzero alerts within validation budget'
else:
    threshold=float(np.nextafter(validation_probability.max(),np.inf))
    threshold_status='no nonzero-alert threshold satisfies the validation budget; zero alerts selected'
validation_flags=validation_probability>=threshold
assert float(validation_flags.mean())<=MAX_VALIDATION_ALERT_RATE+1e-12
print({'threshold':threshold,'status':threshold_status,'validation_alerts':int(validation_flags.sum()),
    'validation_alert_rate':float(validation_flags.mean()),'validation_fraud_recall':float(recall_score(y_validation,validation_flags,zero_division=0))})
print('The selected estimator is not refitted after this threshold selection.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The threshold is chosen using validation only. A zero-alert result is explicit; it is not disguised as successful fraud detection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Evaluate on the later held-out period
# MAGIC 
# MAGIC Report missed frauds and false alarms alongside AP/ROC and probability reliability. The all-legitimate baseline can have impressive accuracy while detecting no fraud. Weighted-model probabilities are not automatically calibrated likelihoods.

# COMMAND ----------

def metric_report(labels,probability,cutoff):
    flags=probability>=cutoff
    tn,fp,fn,tp=confusion_matrix(labels,flags,labels=[0,1]).ravel()
    return {'average_precision':float(average_precision_score(labels,probability)),
        'roc_auc':float(roc_auc_score(labels,probability)),'accuracy':float(accuracy_score(labels,flags)),
        'fraud_precision':float(precision_score(labels,flags,zero_division=0)),
        'fraud_recall':float(recall_score(labels,flags,zero_division=0)),
        'fraud_f1':float(f1_score(labels,flags,zero_division=0)),'alert_rate':float(flags.mean()),
        'false_positive_rate':float(fp/(fp+tn)),'brier':float(brier_score_loss(labels,probability)),
        'tn':int(tn),'fp':int(fp),'fn':int(fn),'tp':int(tp)}

test_probability=selected_model.predict_proba(X_test)[:,1]
test_metrics=metric_report(y_test,test_probability,threshold)
baseline_probability=baseline.predict_proba(X_test)[:,1]
baseline_metrics=metric_report(y_test,baseline_probability,0.5)
print('Model test metrics:',test_metrics)
print('Always-legitimate baseline:',baseline_metrics)
print('Held-out fraud prevalence / constant-score AP reference:',float(y_test.mean()))
assert sum(test_metrics[k] for k in ['tn','fp','fn','tp'])==len(y_test)
assert np.isfinite(test_probability).all() and ((0<=test_probability)&(test_probability<=1)).all()
print('Observed test alert rate need not meet the validation budget; do not retune the cutoff using test labels.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Metrics are computed on the natural held-out class distribution, with no resampling or minimum score guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 — Measure top-budget ranking and plot errors
# MAGIC 
# MAGIC Precision/recall at a fixed top-k review volume is an evaluation metric, not a new test-tuned threshold. Unlike the threshold policy, top-k breaks equal-score ties using the stable transaction-key order. Do not confuse the two review policies.

# COMMAND ----------

budget_k=max(1,int(np.floor(MAX_VALIDATION_ALERT_RATE*len(y_test))))
ranking=np.lexsort((test_keys,-test_probability))
reviewed=ranking[:budget_k]
top_metrics={'k':budget_k,'precision_at_k':float(y_test[reviewed].mean()),
    'recall_at_k':float(y_test[reviewed].sum()/y_test.sum())}
print('Held-out ranking review budget:',top_metrics)
import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay,PrecisionRecallDisplay,RocCurveDisplay
fig,axes=plt.subplots(1,3,figsize=(15,4))
PrecisionRecallDisplay.from_predictions(y_test,test_probability,ax=axes[0])
axes[0].axhline(y_test.mean(),color='gray',linestyle='--',label='fraud prevalence')
axes[0].legend()
RocCurveDisplay.from_predictions(y_test,test_probability,ax=axes[1])
ConfusionMatrixDisplay.from_predictions(y_test,test_probability>=threshold,labels=[0,1],display_labels=['non-fraud','fraud'],ax=axes[2],colorbar=False)
fig.suptitle('Later-period fraud evaluation — historical sample')
plt.tight_layout();plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Ranking and threshold policies are reported separately; figures use actual runtime source labels.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 — Inspect features and false positives/negatives
# MAGIC 
# MAGIC PCA coordinates are anonymized. Their importance cannot be translated back into named cardholder behaviors using this export. The amount bucket has no exact monetary value. Review only hashed source keys, not sensitive card identifiers.

# COMMAND ----------

classifier=selected_model['classifier']
transformed_names=selected_model['preprocessor'].get_feature_names_out(FEATURE_ORDER)
if hasattr(classifier,'coef_'):importance=np.abs(classifier.coef_[0])
else:importance=classifier.feature_importances_
ranked_features=np.argsort(-importance)[:10]
print('Most influential transformed features:',[(str(transformed_names[i]),float(importance[i])) for i in ranked_features])
flags=test_probability>=threshold
for kind,indices in [('false_positive',np.flatnonzero(flags&(y_test==0))),('false_negative',np.flatnonzero((~flags)&(y_test==1)))]:
    for i in indices[:5]:print({'error_type':kind,'transaction_key':test_keys[i],'model_fraud_score':float(test_probability[i])})

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Feature/error inspection refers only to the actual fitted model and source-derived transaction hashes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 — Package the exact preprocessing and review cutoff
# MAGIC 
# MAGIC Input is a 29-column float tensor: 28 PCA coordinates in order, followed by the categorical amount-range code. Output is `[model_fraud_score, review_flag]`. Scores are not certified/calibrated fraud probabilities. The wrapper does not authorize any payment action.

# COMMAND ----------

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature

class FraudReviewModel(mlflow.pyfunc.PythonModel):
    def __init__(self,model,cutoff):self.model=model;self.cutoff=float(cutoff)
    def predict(self,context,model_input,params=None):
        values=np.asarray(model_input,dtype=np.float64)
        if values.ndim!=2 or values.shape[1]!=29:raise ValueError('Expected ordered PCA_1..28 plus amount_range (29 columns)')
        if not np.isfinite(values).all():raise ValueError('All model inputs must be finite')
        if not np.isin(values[:,28],np.arange(8)).all():raise ValueError('amount_range must be an integer category 0–7')
        if len(values)==0:return np.empty((0,2),dtype=float)
        probability=self.model.predict_proba(values)[:,1]
        return np.column_stack([probability,(probability>=self.cutoff).astype(float)])

packaged=FraudReviewModel(selected_model,threshold)
example=X_validation[:3]
example_output=packaged.predict(None,example)
assert example_output.shape==(3,2)
assert np.array_equal(packaged.predict(None,X_test)[:,1],(test_probability>=threshold).astype(float))
print('Threshold-aware review model contract passed')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The packaged model produces the exact review flags evaluated above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 — Log the complete training/evaluation record in MLflow
# MAGIC 
# MAGIC Record source contract, exclusions, chronology, class balance, fold details, model choice, threshold policy, dependencies and test results. Source data is not uploaded to GitHub. Input-example logging is off by default; a signed model is still stored.

# COMMAND ----------

source_fingerprint=hashlib.sha256(json.dumps(list(zip(transaction_keys.tolist(),y.tolist())),separators=(',',':')).encode()).hexdigest()
provenance={'source_path':source_path,'source_rows':source_rows,'missing_vector_rows':missing_vector_rows,
    'other_rejected_rows':invalid_rows,'unique_source_rows':unique_rows,'retained_earliest_rows':len(y),
    'retained_content_sha256':source_fingerprint,'feature_order':FEATURE_ORDER,'time_used_as_feature':False,
    'fit_end_source_time':float(fit_end),'validation_end_source_time':float(validation_end),
    'precomputed_PCA_fitting_provenance':'not exposed by source; cannot verify its upstream fitting split',
    'transaction_key':'content hash of source time, amount bucket, PCA coordinates; not a bank transaction ID'}
if TRACK_WITH_MLFLOW:
    user=spark.sql('SELECT current_user()').first()[0]
    mlflow.set_tracking_uri('databricks')
    mlflow.set_experiment(f'/Users/{user}/credit_card_fraud_learning')
    with mlflow.start_run(run_name='chronological_fraud_review_classifier') as run:
        mlflow.log_params({'seed':SEED,'fit_rows':len(y_fit),'validation_rows':len(y_validation),'test_rows':len(y_test),
            'fit_fraud':int(y_fit.sum()),'validation_fraud':int(y_validation.sum()),'test_fraud':int(y_test.sum()),
            'classifier':type(classifier).__name__,'selection_metric':'training_forward_cv_average_precision',
            'threshold':threshold,'validation_max_alert_rate':MAX_VALIDATION_ALERT_RATE,
            'threshold_status':threshold_status,'refit_after_threshold_selection':False,'synthetic_or_resampled_evaluation':False})
        mlflow.log_metrics({'training_cv_best_average_precision':float(search.best_score_),
            'validation_alert_rate':float(validation_flags.mean()),
            **{'test_'+k:v for k,v in test_metrics.items()},**{'baseline_test_'+k:v for k,v in baseline_metrics.items()},
            **{'test_ranking_'+k:v for k,v in top_metrics.items()}})
        mlflow.log_dict(provenance,'provenance.json')
        mlflow.log_dict({'folds':fold_manifest,'candidates':comparison},'training_cv.json')
        mlflow.log_dict({'versions':versions,'output_columns':['model_fraud_score','review_flag'],
            'intended_use':'education/manual review demonstration only'},'model_contract.json')
        mlflow.log_figure(fig,'held_out_evaluation.png')
        kwargs={'input_example':example} if LOG_INPUT_EXAMPLE else {}
        info=mlflow.pyfunc.log_model(artifact_path='fraud_review_classifier',python_model=packaged,
            signature=infer_signature(example,example_output),
            pip_requirements=[f'{name}=={versions[name]}' for name in ['numpy','scipy','scikit-learn','mlflow']],**kwargs)
        run_id=run.info.run_id;model_uri=info.model_uri
    print('MLflow run:',run_id,'model:',model_uri)
else:
    run_id=None;model_uri=None
    print('Tracking explicitly disabled; no saved artifact is claimed.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** A real MLflow run must exist before logging is reported as successful; no test score is fabricated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15 — Reload and score only actual held-out transactions
# MAGIC 
# MAGIC Check artifact reproducibility using genuine held-out feature rows. A round-trip check proves packaging consistency, not general fraud-detection effectiveness.

# COMMAND ----------

if TRACK_WITH_MLFLOW:
    loaded=mlflow.pyfunc.load_model(model_uri)
    scored=loaded.predict(X_test[:5])
    assert np.allclose(scored,packaged.predict(None,X_test[:5]))
    print('Saved model round-trip passed')
else:
    scored=packaged.predict(None,X_test[:5])
for i,row in enumerate(scored):print({'transaction_key':test_keys[i],'model_fraud_score':float(row[0]),'review_flag':int(row[1])})

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** No fabricated transaction profile is used; only source hashes, scores and review flags are displayed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16 — Optional governed batch output
# MAGIC 
# MAGIC Persist source-derived held-out predictions only if requested through the explicit switch. Keep the model URI/run ID for traceability. No payment network, alert recipient or transaction-blocking endpoint is connected.

# COMMAND ----------

dbutils.widgets.text('target_catalog','')
if SAVE_PREDICTIONS:
    if not TRACK_WITH_MLFLOW:raise ValueError('Enable tracking first for a durable model reference')
    target_catalog=dbutils.widgets.get('target_catalog').strip() or spark.sql('SELECT current_catalog()').first()[0]
    if target_catalog=='samples':raise ValueError('Choose a writable catalog, not samples')
    schema='fraud_learning_'+hashlib.sha256(user.encode()).hexdigest()[:10]
    def quote(value):return '`'+value.replace('`','``')+'`'
    namespace=f'{quote(target_catalog)}.{quote(schema)}'
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
    all_scores=loaded.predict(X_test)
    records=[(str(test_keys[i]),float(value[0]),int(value[1]),int(y_test[i]),run_id) for i,value in enumerate(all_scores)]
    output=spark.createDataFrame(records,'transaction_key string, model_fraud_score double, review_flag int, actual_fraud_label int, run_id string')
    target=f'{namespace}.`held_out_fraud_predictions`'
    output.withColumn('scored_at',F.current_timestamp()).write.format('delta').mode('overwrite').saveAsTable(target)
    assert spark.table(target).count()==len(y_test)
    print('Course output table:',target)
else:
    print('Optional prediction-table write skipped.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Only the explicitly enabled course table is written; source assets remain unchanged.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice and solution guidance
# MAGIC 
# MAGIC 1. **Why is accuracy insufficient?** A classifier that labels everything legitimate can score highly when fraud is rare while finding no fraud. Compare AP, fraud recall/precision, false positives and review volume.
# MAGIC 2. **Why not balance the test set?** Altering prevalence changes precision/AP and hides the actual review workload. Apply training-only weighting and retain the observed evaluation distribution.
# MAGIC 3. **Why keep equal-time transactions together?** It gives strictly ordered source-time periods/folds. It still cannot establish independent cardholder groups, because those identities are not exposed.
# MAGIC 4. **What if the review budget changes?** Select a new operating threshold on development/validation data under an agreed policy. Do not optimize against the same test outcomes and call it a fresh evaluation.
# MAGIC 5. **Can amountRange estimate dollars lost?** No exact transaction amount is supplied. Avoid inventing loss values or treating category codes as money. A production cost model needs actual authorized amounts and agreed outcome costs.
# MAGIC 6. **Are scores calibrated risk probabilities?** Not necessarily, especially after class weighting. Calibration requires a separate chronological training/calibration design and evaluation of probability reliability.
# MAGIC 
# MAGIC ## Dataset and deployment limits
# MAGIC 
# MAGIC This is a historical anonymized sample with supplied PCA coordinates. The raw features and upstream PCA-fitting provenance are not available, so downstream chronological splitting cannot prove that every upstream transformation was trained only on the past. There is no reliable source transaction/cardholder identity, current fraud population, delayed-chargeback process, prospective trial or cost model. Source time is relative seconds, not a reconstructed calendar date. The observed slice is intentionally capped and can differ from the full dataset.
# MAGIC 
# MAGIC Retain the dataset attribution and inspect the asset’s bundled description/license files before using derived artifacts beyond the learning exercise. The original research attribution is Andrea Dal Pozzolo, Olivier Caelen, Reid A. Johnson and Gianluca Bontempi, *Calibrating Probability with Undersampling for Unbalanced Classification* (CIDM, 2015).
# MAGIC 
# MAGIC Before an operational system, define decision authority, label delay, human review, idempotent ingestion/scoring, model/version lineage, calibration, drift monitoring and representative prospective evaluation. This notebook implements a learning model and optional course outputs; it does not implement automatic payment decisions.
# MAGIC 
# MAGIC If the asset preflight fails, stop and resolve the Databricks source requirement. Do not silently replace the asset with a web CSV, generated records or a different schema.
