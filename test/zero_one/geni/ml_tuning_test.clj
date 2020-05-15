(ns zero-one.geni.ml-tuning-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.ml-tuning :as ml-tuning])
  (:import
    (org.apache.spark.ml.tuning CrossValidator)))

(facts "On field reflection"
  (let [stage (ml/hashing-tf {})]
    (fact "should get the correct fields."
      (interop/get-field stage :binary) => (.binary stage)
      (interop/get-field stage :input-col) => (.inputCol stage)
      (interop/get-field stage :num-features) => (.numFeatures stage)
      (interop/get-field stage :output-col) => (.outputCol stage))))

(facts "On param grid builder"
  (fact "should be able to replicate Spark example."
    (let [hashing-tf (ml/hashing-tf {:input-col "words" :output-col "features"})
          log-reg    (ml/logistic-regression {:max-iter 10})
          param-grid (ml-tuning/param-grid
                       {hashing-tf {:num-features [10 100 1000]}
                        log-reg    {:reg-param [0.1 0.01] :max-iter [1 2 3]}})]
      param-grid => #(-> % class .isArray)
      (count param-grid) => 18
      (every? #(= (.size %) 3) param-grid) => true)))

(facts "On cross validator"
  (fact "should be instantiatable"
    (ml-tuning/cross-validator {}) => #(instance? CrossValidator %))
  (fact "should be able to replicate Spark example."
    (let [log-reg    (ml/logistic-regression {:max-iter 1})
          param-grid (ml-tuning/param-grid {log-reg {:reg-param [0.1]}})
          cv         (ml-tuning/cross-validator
                       {:estimator log-reg
                        :evaluator (ml/binary-classification-evaluator {})
                        :estimator-param-maps param-grid
                        :num-folds 222
                        :parallelism 101})
          cv-params (ml/params cv)]
      (:num-folds cv-params) => 222
      (:parallelism cv-params) => 101)))
