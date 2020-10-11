(ns zero-one.geni.ml-tuning-test
  (:require
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [libsvm-df]])
  (:import
   (org.apache.spark.ml.classification LogisticRegressionModel)
   (org.apache.spark.ml.tuning CrossValidator
                               TrainValidationSplit)))

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
          param-grid (ml/param-grid
                      {hashing-tf {:num-features [10 100 1000]}
                       log-reg    {:reg-param [0.1 0.01] :max-iter [1 2 3]}})]
      param-grid => #(-> % class .isArray)
      (count param-grid) => 18
      (every? #(= (.size %) 3) param-grid) => true)))

(facts "On cross validator fitting" :slow
  (fact "should be able to replicate Spark example."
    (let [log-reg    (ml/logistic-regression {:max-iter 1})
          param-grid (ml/param-grid {log-reg {:reg-param [0.1]}})
          cv         (ml/cross-validator
                      {:estimator log-reg
                       :estimator-param-maps param-grid
                       :evaluator (ml/binary-classification-evaluator {})
                       :num-folds 2})
          model      (ml/fit libsvm-df cv)]
      (ml/best-model model) => (partial instance? LogisticRegressionModel))))

(facts "On cross validator"
  (fact "should be instantiatable"
    (ml/cross-validator {}) => #(instance? CrossValidator %))
  (fact "should be able to replicate Spark example."
    (let [log-reg    (ml/logistic-regression {:max-iter 1})
          param-grid (ml/param-grid {log-reg {:reg-param [0.1]}})
          cv         (ml/cross-validator
                      {:estimator log-reg
                       :evaluator (ml/binary-classification-evaluator {})
                       :estimator-param-maps param-grid
                       :num-folds 222
                       :seed 112233
                       :parallelism 101})
          cv-params (ml/params cv)]
      (:seed cv-params) => 112233
      (:num-folds cv-params) => 222
      (:parallelism cv-params) => 101)))

(facts "On train-validation split"
  (fact "should be instantiatable"
    (ml/train-validation-split {}) => #(instance? TrainValidationSplit %))
  (fact "should be able to replicate Spark example."
    (let [split        (ml/train-validation-split
                        {:estimator (ml/logistic-regression {})
                         :evaluator (ml/binary-classification-evaluator {})
                         :estimator-param-maps (ml/param-grid {})
                         :seed 888
                         :parallelism 777})
          split-params (ml/params split)]
      (:seed split-params) => 888
      (:parallelism split-params) => 777)))
