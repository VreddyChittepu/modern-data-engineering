# Databricks notebook source
# MAGIC %md
# MAGIC # Sentiment analysis · Databricks reviews + MLflow
# MAGIC 
# MAGIC Learn how to turn review text into numerical features and classify it as negative or positive. Spark reads the Databricks assets; a bounded scikit-learn pipeline trains on the driver. No pandas, downloaded dataset, generated review or pretrained-model download is used.
# MAGIC 
# MAGIC **Source candidates:** `/databricks-datasets/amazon/data20K` and `/databricks-datasets/amazon/test4K`, with columns `review` and `rating`. Individual workspace availability must pass preflight. Only Databricks-provided asset locations are allowed; no external fallback is provided.
# MAGIC 
# MAGIC **Label definition:** actual 1–2 star ratings → negative (0), actual 4–5 star ratings → positive (1). Exclude 3 stars from this binary task. This is **rating-derived weak supervision**, not independently annotated sentiment. Three-star text is not automatically neutral, and high/low ratings can disagree with the wording.
# MAGIC 
# MAGIC **Setup:** add compatible `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4`, and `mlflow>=2.20,<4` in the Databricks notebook Environment panel. Apply/restart and run top to bottom. Use the PySpark supplied by Databricks. The optional SQL AI comparison requires supported compute/region and is off by default.
# MAGIC 
# MAGIC You will check the source, clean/deduplicate text, hold out evaluation records, fit TF-IDF and two classifiers, choose a validation cutoff, evaluate against a baseline, explain learned features, and log/reload the same threshold-aware model in MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configure a bounded learning run
# MAGIC 
# MAGIC These limits cap driver data, not the source scan/sort cost. Actual retained counts will change after quality and duplicate checks.

# COMMAND ----------

import re, html, json, hashlib
from pathlib import PurePosixPath
from importlib.metadata import version
import numpy as np
from pyspark.sql import functions as F, types as T

SEED=42
MAX_DEVELOPMENT=8000
MAX_TEST=4000
MAX_REVIEW_CHARS=12000
TRACK_WITH_MLFLOW=True
LOG_RAW_INPUT_EXAMPLE=False
PRINT_REVIEW_TEXT=False
SAVE_PREDICTIONS=False
RUN_AI_FUNCTION=False
dbutils.widgets.text('asset_root','/databricks-datasets/amazon')
asset_root=dbutils.widgets.get('asset_root').strip()
versions={name:version(name) for name in ['numpy','scipy','scikit-learn','matplotlib','mlflow']}
print('Versions:',versions)
print('Caps:',MAX_DEVELOPMENT,MAX_TEST,'maximum review characters:',MAX_REVIEW_CHARS)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** No dataset download or model endpoint call runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Check the Databricks asset boundary
# MAGIC 
# MAGIC The default is a documented legacy sample path. If your workspace instead exposes these same assets in `/Volumes/samples/databricks/datasets/`, browse that official volume and enter the observed parent directory. Do not guess a custom volume or upload another dataset.

# COMMAND ----------

def validate_asset_root(value):
    if not isinstance(value,str): raise ValueError('asset_root must be text')
    canonical=value.removeprefix('dbfs:').rstrip('/')
    prefixes=('/databricks-datasets/','/Volumes/samples/databricks/datasets/')
    if not any(canonical.startswith(p) for p in prefixes):
        raise ValueError('Only Databricks-provided sample asset paths are allowed')
    if '..' in PurePosixPath(canonical).parts or '://' in canonical or '\\' in canonical:
        raise ValueError('Noncanonical asset path rejected')
    return canonical

asset_root=validate_asset_root(asset_root)
paths={'development':asset_root+'/data20K','candidate_test':asset_root+'/test4K'}
try:
    assets={name:spark.read.parquet(path) for name,path in paths.items()}
    for name,frame in assets.items():
        if not {'review','rating'}.issubset(frame.columns): raise ValueError(f'{name} requires review and rating columns')
        if not isinstance(frame.schema['review'].dataType,T.StringType): raise ValueError('review must be string')
        if frame.limit(1).count()!=1: raise ValueError(f'{name} is empty')
except Exception as exc:
    raise RuntimeError('Required Databricks Amazon review assets are unavailable or incompatible. '
        'Resolve access/schema first; no external or generated replacement is loaded.') from exc
print('Asset read/schema preflight passed:',paths)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Both Parquet datasets must be readable and contain real review text and ratings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Normalize text without removing negation
# MAGIC 
# MAGIC Normalize HTML entities, tags, repeated whitespace and case. Keep words such as not/no; blindly removing stop words can discard important sentiment. This exact function will also run inside the fitted model at inference.

# COMMAND ----------

def normalize_review(text):
    if not isinstance(text,str): raise ValueError('Each review must be a string')
    if len(text)>MAX_REVIEW_CHARS: raise ValueError('Review exceeds the documented input length cap')
    decoded=html.unescape(text)
    without_tags=re.sub(r'<[^>]+>',' ',decoded)
    return re.sub(r'\s+',' ',without_tags).strip().lower()

def label_from_rating(value):
    if type(value) is not int: raise ValueError('Rating must be an integer star value')
    if value in (1,2): return 0
    if value in (4,5): return 1
    if value==3: return None
    raise ValueError('Expected an integer star rating from 1 through 5')
print('Rating policy: 1/2 negative; 4/5 positive; 3 excluded.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The normalization and label policy are explicit. They do not infer labels by looking for sentiment words in the input.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Validate ratings, bound collection and deduplicate
# MAGIC 
# MAGIC Profile all source records, exclude invalid/empty/overlong text and 3-star rows, then collect a deterministic capped slice. For repeated normalized text, retain one label-consistent representative. Remove texts with conflicting positive/negative proxy labels rather than arbitrarily picking one.

# COMMAND ----------

def load_reviews(frame,cap):
    prepared=frame.select('review',F.expr('try_cast(rating AS DOUBLE)').alias('rating'))
    valid=(F.col('review').isNotNull()&(F.length(F.trim('review'))>0)
        &(F.length('review')<=MAX_REVIEW_CHARS)&F.col('rating').isin(1.,2.,3.,4.,5.))
    n=prepared.count()
    usable=prepared.filter(valid)
    valid_n=usable.count()
    three_star=usable.filter('rating = 3').count()
    eligible=(usable.filter('rating <> 3').withColumn('_order',F.sha2('review',256))
        .orderBy('_order','review','rating').limit(cap))
    rows=eligible.select('review','rating').collect()
    buckets={}; blank_after_normalization=0; normalized_overlong=0
    for row in rows:
        text=normalize_review(row['review'])
        if not text:
            blank_after_normalization+=1; continue
        if len(text)>MAX_REVIEW_CHARS:
            normalized_overlong+=1; continue
        key=hashlib.sha256(text.encode()).hexdigest()
        label=label_from_rating(int(row['rating']))
        if key not in buckets: buckets[key]={'text':text,'labels':set(),'occurrences':0}
        buckets[key]['labels'].add(label); buckets[key]['occurrences']+=1
    conflicts=sum(len(v['labels'])!=1 for v in buckets.values())
    conflict_rows=sum(v['occurrences'] for v in buckets.values() if len(v['labels'])!=1)
    duplicate_rows=sum(v['occurrences']-1 for v in buckets.values() if len(v['labels'])==1)
    records=[{'review_id':key,'review':v['text'],'label':next(iter(v['labels']))}
        for key,v in sorted(buckets.items()) if len(v['labels'])==1]
    report={'source_rows':n,'invalid_or_empty_or_overlong_rows':n-valid_n,
        'three_star_excluded':three_star,'eligible_rows':valid_n-three_star,'collected_cap':cap,
        'collected_rows':len(rows),'blank_after_normalization':blank_after_normalization,
        'overlong_after_normalization':normalized_overlong,'conflicting_text_keys_excluded':conflicts,
        'conflicting_text_rows_excluded':conflict_rows,'duplicate_occurrences_removed':duplicate_rows,
        'unique_retained_texts':len(records)}
    assert len(rows)==blank_after_normalization+normalized_overlong+conflict_rows+duplicate_rows+len(records)
    return records,report

development,development_quality=load_reviews(assets['development'],MAX_DEVELOPMENT)
candidate_test,test_quality=load_reviews(assets['candidate_test'],MAX_TEST)
print('Development quality:',development_quality)
print('Candidate-test quality:',test_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Only real asset records are retained. All exclusions/caps are reported; labels are proxies derived only from actual ratings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Prevent duplicate-text leakage across splits
# MAGIC 
# MAGIC Treat test4K as a candidate holdout, not as proof of independence. Exclude any normalized test text already present in development. Author/product identity and near-duplicate leakage remain possible because this baseline uses only review/rating.

# COMMAND ----------

development_ids={r['review_id'] for r in development}
test=[r for r in candidate_test if r['review_id'] not in development_ids]
overlap_removed=len(candidate_test)-len(test)
print('Overlapping normalized test texts removed:',overlap_removed)
if len(development)<100 or len(test)<50:
    raise ValueError('Too few independent retained texts. Review asset overlap/quality; no substitute data is created.')
texts=np.array([r['review'] for r in development],dtype=str)
labels=np.array([r['label'] for r in development],dtype=np.int64)
test_texts=np.array([r['review'] for r in test],dtype=str)
test_labels=np.array([r['label'] for r in test],dtype=np.int64)
if min(np.bincount(labels,minlength=2))<20 or min(np.bincount(test_labels,minlength=2))<5:
    raise ValueError('Both proxy classes need enough real retained records')
from sklearn.model_selection import train_test_split,StratifiedKFold,GridSearchCV
fit_texts,validation_texts,y_fit,y_validation=train_test_split(
    texts,labels,test_size=0.20,stratify=labels,random_state=SEED)
assert not (set(fit_texts)&set(validation_texts))
assert not (set(texts)&set(test_texts))
print('Fit/validation/test sizes:',len(y_fit),len(y_validation),len(test_labels))
print('Fit class counts [negative, positive]:',np.bincount(y_fit,minlength=2).tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Exact normalized text cannot appear on both sides of any evaluation split. The test set is now locked for model selection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Fit a simple prior baseline
# MAGIC 
# MAGIC A prior classifier uses only class frequency from fit data. Its high accuracy under imbalance can be misleading, so compare macro-F1 and class-specific errors too.

# COMMAND ----------

from sklearn.dummy import DummyClassifier
baseline=DummyClassifier(strategy='prior').fit(fit_texts,y_fit)
print('Fit-only class prior:',baseline.class_prior_)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The baseline reads no word features and no test labels during fitting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Build TF-IDF inside the training pipeline
# MAGIC 
# MAGIC TF-IDF creates sparse word/phrase features weighted by frequency and rarity. Unigrams represent single words; bigrams represent adjacent pairs. Vocabulary and IDF must be fitted inside each training fold, never on all reviews before splitting.

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB

vectorizer=TfidfVectorizer(preprocessor=normalize_review,lowercase=False,stop_words=None,
    ngram_range=(1,2),min_df=2,max_features=40000,sublinear_tf=True)
pipeline=Pipeline([('tfidf',vectorizer),('classifier',LogisticRegression(max_iter=1500,solver='liblinear',random_state=SEED))])
grid=[{'classifier':[LogisticRegression(max_iter=1500,solver='liblinear',class_weight='balanced',random_state=SEED)],
       'classifier__C':[0.5,1.0,2.0]},
      {'classifier':[MultinomialNB()],'classifier__alpha':[0.5,1.0]}]
cv=StratifiedKFold(n_splits=3,shuffle=True,random_state=SEED)
search=GridSearchCV(pipeline,grid,scoring={'macro_f1':'f1_macro','roc_auc':'roc_auc'},
    refit='macro_f1',cv=cv,n_jobs=1,error_score='raise')
search.fit(fit_texts,y_fit)
selected_model=search.best_estimator_
print('Selected classifier:',type(selected_model['classifier']).__name__)
print('Best training-CV macro-F1:',float(search.best_score_))
print('Vocabulary size:',len(selected_model['tfidf'].vocabulary_))

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Five configurations are compared on fit data only. No target rating or metadata field enters the text feature matrix.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Review candidate performance and sparsity
# MAGIC 
# MAGIC Sparse matrices store nonzero entries instead of a dense document-by-vocabulary array. Keep them sparse rather than calling toarray on the full dataset.

# COMMAND ----------

comparison=[]
for i,params in enumerate(search.cv_results_['params']):
    comparison.append({'classifier':type(params['classifier']).__name__,
        'parameters':{k:str(v) for k,v in params.items() if k.startswith('classifier__')},
        'mean_cv_macro_f1':float(search.cv_results_['mean_test_macro_f1'][i]),
        'sd_cv_macro_f1':float(search.cv_results_['std_test_macro_f1'][i]),
        'mean_cv_roc_auc':float(search.cv_results_['mean_test_roc_auc'][i])})
for row in sorted(comparison,key=lambda r:-r['mean_cv_macro_f1']):print(row)
sparse_preview=selected_model['tfidf'].transform(validation_texts[:10])
print('Sparse preview shape:',sparse_preview.shape,'nonzero entries:',sparse_preview.nnz)
assert len(comparison)==5

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The comparison uses CV statistics, and the validation preview stays sparse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Select a validation cutoff
# MAGIC 
# MAGIC A classifier probability needs a threshold for binary labels. Choose the cutoff by validation macro-F1 so both classes matter. This is a classroom rule, not a universal business threshold. Keep the exact fitted model afterward instead of refitting and changing the validated probability distribution.

# COMMAND ----------

from sklearn.metrics import (f1_score,accuracy_score,precision_score,recall_score,roc_auc_score,
    average_precision_score,confusion_matrix,brier_score_loss,classification_report)
validation_probability=selected_model.predict_proba(validation_texts)[:,1]
thresholds=np.linspace(0.10,0.90,81)
validation_scores=np.array([f1_score(y_validation,validation_probability>=t,average='macro',zero_division=0) for t in thresholds])
best_validation=float(validation_scores.max())
ties=np.flatnonzero(np.isclose(validation_scores,best_validation))
chosen=min(ties,key=lambda i:(abs(float(thresholds[i])-0.5),float(thresholds[i])))
threshold=float(thresholds[chosen])
print('Selected threshold:',threshold,'validation macro-F1:',best_validation)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The cutoff uses validation labels only; no fitting follows this selection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Evaluate on the locked asset-derived holdout
# MAGIC 
# MAGIC Accuracy measures overall correctness; macro-F1 weights the two classes equally. Per-class precision/recall and the confusion matrix make imbalance visible. AP below uses positive as the positive class. Agreement is against rating proxies, not gold-standard human sentiment.

# COMMAND ----------

def evaluate(labels,probability,cutoff):
    prediction=probability>=cutoff
    tn,fp,fn,tp=confusion_matrix(labels,prediction,labels=[0,1]).ravel()
    return {'accuracy':float(accuracy_score(labels,prediction)),
        'macro_f1':float(f1_score(labels,prediction,average='macro',zero_division=0)),
        'positive_precision':float(precision_score(labels,prediction,zero_division=0)),
        'positive_recall':float(recall_score(labels,prediction,zero_division=0)),
        'roc_auc':float(roc_auc_score(labels,probability)),
        'average_precision_positive':float(average_precision_score(labels,probability)),
        'brier':float(brier_score_loss(labels,probability)),
        'tn':int(tn),'fp':int(fp),'fn':int(fn),'tp':int(tp)}

test_probability=selected_model.predict_proba(test_texts)[:,1]
test_metrics=evaluate(test_labels,test_probability,threshold)
baseline_metrics=evaluate(test_labels,baseline.predict_proba(test_texts)[:,1],0.5)
print('Model metrics:',test_metrics)
print('Prior baseline:',baseline_metrics)
print(classification_report(test_labels,test_probability>=threshold,labels=[0,1],target_names=['negative proxy','positive proxy'],zero_division=0))
assert sum(test_metrics[k] for k in ['tn','fp','fn','tp'])==len(test_labels)
assert np.isfinite(test_probability).all() and ((test_probability>=0)&(test_probability<=1)).all()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Actual held-out scores are reported without a guaranteed minimum accuracy assertion. Reusing test feedback for tuning makes the test exploratory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 — Plot ranking and error patterns
# MAGIC 
# MAGIC These figures are generated only after real asset training. Positive probability is a model estimate and is not automatically calibrated confidence.

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay,RocCurveDisplay,PrecisionRecallDisplay
fig,axes=plt.subplots(1,3,figsize=(15,4))
ConfusionMatrixDisplay.from_predictions(test_labels,test_probability>=threshold,labels=[0,1],
    display_labels=['negative','positive'],ax=axes[0],colorbar=False)
RocCurveDisplay.from_predictions(test_labels,test_probability,ax=axes[1])
PrecisionRecallDisplay.from_predictions(test_labels,test_probability,ax=axes[2])
fig.suptitle('Held-out review rating-proxy evaluation')
plt.tight_layout();plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Confusion, ROC and PR plots use the locked holdout’s actual proxy labels.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 — Inspect learned features and review errors
# MAGIC 
# MAGIC Coefficients/log-probability differences identify associations, not causal explanations or a complete understanding of language. Inspect difficult reviews only within the permitted learning workspace. Raw text printing is off by default.

# COMMAND ----------

classifier=selected_model['classifier']
names=selected_model['tfidf'].get_feature_names_out()
if hasattr(classifier,'coef_'): weights=classifier.coef_[0]
else: weights=classifier.feature_log_prob_[1]-classifier.feature_log_prob_[0]
print('Negative-associated features:',names[np.argsort(weights)[:12]].tolist())
print('Positive-associated features:',names[np.argsort(-weights)[:12]].tolist())
errors=np.flatnonzero((test_probability>=threshold).astype(int)!=test_labels)
for i in errors[:5]:
    result={'review_id':test[i]['review_id'],'actual_proxy_label':int(test_labels[i]),
        'positive_probability':float(test_probability[i]),'predicted_label':int(test_probability[i]>=threshold)}
    if PRINT_REVIEW_TEXT: result['review_excerpt']=test_texts[i][:300]
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Terms and error IDs come from the actual asset/model, never invented example reviews. Sarcasm, mixed opinions and domain shifts remain limitations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 — Package raw-text preprocessing and the threshold
# MAGIC 
# MAGIC The wrapper accepts a one-dimensional NumPy string tensor and returns two numeric columns. Training and inference share the fitted TF-IDF pipeline and the selected threshold. Whitespace-only input fails instead of returning a misleading confident label.

# COMMAND ----------

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature

class ThresholdedSentimentModel(mlflow.pyfunc.PythonModel):
    def __init__(self,model,cutoff): self.model=model;self.cutoff=float(cutoff)
    def predict(self,context,model_input,params=None):
        values=np.asarray(model_input)
        if values.ndim!=1: raise ValueError('Expected a one-dimensional tensor of review strings')
        for value in values:
            if not isinstance(value,str) or not normalize_review(value): raise ValueError('Review must be a nonblank string within the length cap')
        if len(values)==0:return np.empty((0,2),dtype=float)
        probability=self.model.predict_proba(values)[:,1]
        return np.column_stack([probability,(probability>=self.cutoff).astype(float)])

packaged=ThresholdedSentimentModel(selected_model,threshold)
example=validation_texts[:3]
example_output=packaged.predict(None,example)
assert example_output.shape==(3,2)
assert np.array_equal(packaged.predict(None,test_texts)[:,1],(test_probability>=threshold).astype(float))
print('Output columns: positive_probability, predicted_rating_proxy_label (0 negative / 1 positive)')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** The packaged labels exactly match the evaluated thresholded predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 — Track and reload the model with MLflow
# MAGIC 
# MAGIC Store source paths, data fingerprints, quality exclusions, CV summaries, dependencies, chosen cutoff and test results. Raw review input examples are not logged unless LOG_RAW_INPUT_EXAMPLE is explicitly enabled. A text model’s learned vocabulary still reflects training tokens.

# COMMAND ----------

def records_fingerprint(records):
    payload=json.dumps([(r['review_id'],r['label']) for r in records],separators=(',',':'))
    return hashlib.sha256(payload.encode()).hexdigest()
provenance={'paths':paths,'development_quality':development_quality,'candidate_test_quality':test_quality,
    'overlap_test_texts_removed':overlap_removed,'development_sha256':records_fingerprint(development),
    'retained_test_sha256':records_fingerprint(test),'label_policy':'1/2 negative; 4/5 positive; exclude 3',
    'test_definition':'capped cleaned test4K records with development text overlap excluded'}
if TRACK_WITH_MLFLOW:
    user=spark.sql('SELECT current_user()').first()[0]
    mlflow.set_tracking_uri('databricks')
    mlflow.set_experiment(f'/Users/{user}/sentiment_analysis_learning')
    with mlflow.start_run(run_name='amazon_rating_proxy_sentiment') as run:
        mlflow.log_params({'seed':SEED,'fit_rows':len(y_fit),'validation_rows':len(y_validation),'test_rows':len(test_labels),
            'classifier':type(classifier).__name__,'threshold':threshold,'max_features':40000,'ngrams':'1,2',
            'cv_folds':3,'selection_metric':'fit_cv_macro_f1','refit_after_threshold_selection':False})
        mlflow.log_metrics({'fit_cv_best_macro_f1':float(search.best_score_),'validation_macro_f1':best_validation,
            **{'test_'+k:v for k,v in test_metrics.items()},**{'baseline_test_'+k:v for k,v in baseline_metrics.items()}})
        mlflow.log_dict(provenance,'provenance.json')
        mlflow.log_dict({'candidates':comparison},'training_cv_comparison.json')
        mlflow.log_dict({'versions':versions,'max_review_chars':MAX_REVIEW_CHARS,'normalization':'HTML unescape/tag removal/whitespace/lowercase'},'environment_and_contract.json')
        mlflow.log_figure(fig,'held_out_evaluation.png')
        kwargs={'input_example':example} if LOG_RAW_INPUT_EXAMPLE else {}
        info=mlflow.pyfunc.log_model(artifact_path='sentiment_classifier',python_model=packaged,
            signature=infer_signature(example,example_output),
            pip_requirements=[f'{name}=={versions[name]}' for name in ['numpy','scipy','scikit-learn','mlflow']],**kwargs)
        run_id=run.info.run_id; model_uri=info.model_uri
    loaded=mlflow.pyfunc.load_model(model_uri)
    scored=loaded.predict(test_texts[:5])
    assert np.allclose(scored,packaged.predict(None,test_texts[:5]))
    print('MLflow run:',run_id,'model:',model_uri,'artifact round-trip passed')
else:
    run_id=None;model_uri=None;scored=packaged.predict(None,test_texts[:5])
    print('Tracking explicitly disabled; no saved model is claimed.')
for i,row in enumerate(scored):print({'review_id':test[i]['review_id'],'positive_probability':float(row[0]),'label':int(row[1])})

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Tracking success requires a real run and matching reloaded outputs; no placeholder metrics or artifact URI is substituted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15 — Optional Databricks AI sentiment comparison
# MAGIC 
# MAGIC This separate path calls a live Databricks SQL AI function on at most ten real held-out asset reviews. It can consume quota/incur cost and requires a supported region/compute. It is off by default. Its positive/negative/neutral/mixed categories do not match the binary rating-proxy task exactly.

# COMMAND ----------

if RUN_AI_FUNCTION:
    rows=[(r['review_id'],r['review'],int(r['label'])) for r in test[:10]]
    ai_input=spark.createDataFrame(rows,'review_id string, review string, rating_proxy_label int')
    # Single action: avoid accidentally repeating inference through separate display/count/collect calls.
    ai_results=ai_input.select('review_id','rating_proxy_label',F.expr('ai_analyze_sentiment(review)').alias('ai_sentiment')).collect()
    comparable=[]
    for row in ai_results:
        value=row['ai_sentiment']
        if value in ('positive','negative'):
            comparable.append((row['rating_proxy_label'],int(value=='positive')))
        print(row.asDict())
    print('Binary-comparable coverage:',len(comparable),'of',len(ai_results))
    if comparable:
        print('Agreement with rating proxy among comparable rows:',sum(a==b for a,b in comparable)/len(comparable))
else:
    print('Live AI function skipped; the supervised model needs no model endpoint or LLM download.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Enabled mode reports real function responses and coverage. Neutral, mixed or null responses are not forced into a binary label; this tiny comparison is not a benchmark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16 — Optional course-only batch output
# MAGIC 
# MAGIC Write predictions from actual held-out asset records only. No full review text is persisted by this block. This does not create a serving endpoint or change the sample assets.

# COMMAND ----------

dbutils.widgets.text('target_catalog','')
if SAVE_PREDICTIONS:
    if not TRACK_WITH_MLFLOW:raise ValueError('Enable tracking first for a durable model reference')
    target_catalog=dbutils.widgets.get('target_catalog').strip() or spark.sql('SELECT current_catalog()').first()[0]
    if target_catalog=='samples':raise ValueError('Choose a writable catalog, not samples')
    schema='sentiment_learning_'+hashlib.sha256(user.encode()).hexdigest()[:10]
    def quote(value):return '`'+value.replace('`','``')+'`'
    namespace=f'{quote(target_catalog)}.{quote(schema)}'
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
    all_scores=loaded.predict(test_texts)
    rows=[(test[i]['review_id'],float(value[0]),int(value[1]),int(test_labels[i]),run_id) for i,value in enumerate(all_scores)]
    output=spark.createDataFrame(rows,'review_id string, positive_probability double, predicted_proxy_label int, actual_proxy_label int, run_id string')
    target=f'{namespace}.`amazon_test_predictions`'
    output.withColumn('scored_at',F.current_timestamp()).write.format('delta').mode('overwrite').saveAsTable(target)
    assert spark.table(target).count()==len(test_labels)
    print('Course prediction table:',target)
else:
    print('Prediction-table output skipped.')

# COMMAND ----------

# MAGIC %md
# MAGIC **Check:** Only the named per-user course output is written when enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice and solution guidance
# MAGIC 
# MAGIC 1. **Why exclude 3-star rows?** This binary label policy lacks a reliable neutral class. Three-star reviews may express mixed or strongly worded opinions; a star-to-neutral rule is not human sentiment annotation.
# MAGIC 2. **Why fit TF-IDF inside CV?** The vocabulary and IDF describe the training distribution. Fitting them on held-out reviews leaks information even without their labels.
# MAGIC 3. **What do bigrams add?** They can capture adjacent word patterns, including local negation. They still struggle with sarcasm, long-range context and mixed sentiment.
# MAGIC 4. **What should happen to exact duplicate text?** It must not cross a split. Author/product/time grouping and near-duplicate checks are useful extensions when metadata is available.
# MAGIC 5. **How would you scale?** Keep ingestion/quality checks distributed, use Spark ML text pipelines or a governed inference service, and avoid collecting a large review corpus. This driver-side example is explicitly capped.
# MAGIC 6. **How would you validate real sentiment?** Create a separately reviewed, representative labeled set with an annotation rubric. Rating-proxy agreement and an optional LLM comparison do not replace human gold labels.
# MAGIC 
# MAGIC The repository contains no invented prediction examples, copied source reviews or claimed score. The notebook’s asset access, training, charts and MLflow operations require runtime verification in your Databricks workspace. If the required assets are unavailable, stop at preflight instead of downloading/replacing the dataset.
