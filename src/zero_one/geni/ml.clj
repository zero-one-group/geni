(ns zero-one.geni.ml
  (:import
    (org.apache.spark.ml.feature VectorAssembler)
    (org.apache.spark.ml.stat Correlation)))

(defn transform [dataframe transformer]
  (.transform transformer dataframe))

(defn vector-assembler [{:keys [input-cols output-col]}]
  (-> (VectorAssembler.)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCol output-col)))

(defn corr [dataframe col-name]
  (Correlation/corr dataframe col-name))

(defn vector->seq [spark-vector]
  (-> spark-vector .values seq))

(defn matrix->seqs [matrix]
  (let [n-cols (.numCols matrix)]
    (->> matrix .values seq (partition n-cols))))

(comment

  ; TODO:
  ; Basic Stats: hythosesis testing, summarizer
  ; Pipelines: pipeline

  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.dataset :as ds])

  (def dataset
    (ds/table->dataset
      @g/spark
      [[1.0 0.0 -2.0 0.0]
       [4.0 5.0 0.0  3.0]
       [6.0 7.0 0.0  8.0]
       [9.0 0.0 1.0  0.0]]
      [:a :b :c :d]))

  (->> dataset g/dtypes)

  (def estimator
    (pipeline
      (tokenizer {:input-col "text" :output-col "words"})
      (hashing-tf {:num-features 1000 :input-col "words" :output-col "features"})
      (logistic-regression {:max-iter 10 :reg-param 0.001})))

  (def transformer
    (fit train-dataframe estimator))

  (-> test-dataframe
      (transform transformer)
      (select "probability" "prediction")
      show)


  true)
