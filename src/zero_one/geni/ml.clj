(ns zero-one.geni.ml
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.walk :refer [keywordize-keys]]
    [zero-one.geni.scala :as scala])
  (:import
    (org.apache.spark.ml Pipeline PipelineStage)
    (org.apache.spark.ml.classification LogisticRegression)
    (org.apache.spark.ml.feature HashingTF
                                 Tokenizer
                                 VectorAssembler)
    (org.apache.spark.ml.stat ChiSquareTest
                              Correlation)))

(defn corr [dataframe col-name]
  (Correlation/corr dataframe col-name))

(defn chi-square-test [dataframe features-col label-col]
  (ChiSquareTest/test dataframe features-col label-col))

(defn vector-assembler [{:keys [input-cols output-col]}]
  (-> (VectorAssembler.)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCol output-col)))

(defn tokeniser [{:keys [input-col output-col]}]
  (-> (Tokenizer.)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def tokenizer tokeniser)

(defn hashing-tf [{:keys [binary num-features input-col output-col]
                   :or   {binary false num-features 262144}}]
  (-> (HashingTF.)
      (.setBinary binary)
      (.setNumFeatures num-features)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn logistic-regression [{:keys [max-iter
                                   family
                                   tol
                                   raw-prediction-col
                                   elastic-net-param
                                   reg-param
                                   aggregation-depth
                                   threshold
                                   fit-intercept
                                   label-col
                                   standardization
                                   probability-col
                                   prediction-col
                                   features-col]
                            :or   {max-iter 100,
                                   family "auto",
                                   tol 1.0E-6,
                                   raw-prediction-col "rawPrediction",
                                   elastic-net-param 0.0,
                                   reg-param 0.0,
                                   aggregation-depth 2,
                                   threshold 0.5,
                                   fit-intercept true,
                                   label-col "label",
                                   standardization true,
                                   probability-col "probability",
                                   prediction-col "prediction",
                                   features-col "features"}}]
  (-> (LogisticRegression.)
      (.setMaxIter max-iter)
      (.setFamily family)
      (.setTol tol)
      (.setRawPredictionCol raw-prediction-col)
      (.setElasticNetParam elastic-net-param)
      (.setRegParam reg-param)
      (.setAggregationDepth aggregation-depth)
      (.setThreshold threshold)
      (.setFitIntercept fit-intercept)
      (.setLabelCol label-col)
      (.setStandardization standardization)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)
      (.setFeaturesCol features-col)))

(defn pipeline [& stages]
  (-> (Pipeline.)
      (.setStages (into-array PipelineStage stages))))

(defn fit [dataframe estimator]
  (.fit estimator dataframe))

(defn transform [dataframe transformer]
  (.transform transformer dataframe))

(defn params [stage]
  (let [param-pairs (-> stage .extractParamMap .toSeq scala/scala-seq->vec)
        unpack-pair (fn [p] [(-> p .param .name ->kebab-case) (.value p)])]
    (->> param-pairs
         (map unpack-pair)
         (into {})
         keywordize-keys)))

(defn vector->seq [spark-vector]
  (-> spark-vector .values seq))

(defn matrix->seqs [matrix]
  (let [n-cols (.numCols matrix)]
    (->> matrix .values seq (partition n-cols))))

(comment

  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.dataset :as ds])

  true)
