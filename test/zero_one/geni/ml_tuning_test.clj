(ns ^:classic zero-one.geni.ml-tuning-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [libsvm-df]])
  (:import
   (org.apache.spark.ml.classification LogisticRegressionModel)
   (org.apache.spark.ml.tuning CrossValidator
                               TrainValidationSplit)))

(deftest field-reflection-test
  (let [stage (ml/hashing-tf {})]
    (testing "should get the correct fields."
      (is (= (.binary stage) (interop/get-field stage :binary)))
      (is (= (.inputCol stage) (interop/get-field stage :input-col)))
      (is (= (.numFeatures stage) (interop/get-field stage :num-features)))
      (is (= (.outputCol stage) (interop/get-field stage :output-col))))))

(deftest param-grid-builder-test
  (testing "should be able to replicate Spark example."
    (let [hashing-tf (ml/hashing-tf {:input-col "words" :output-col "features"})
          log-reg    (ml/logistic-regression {:max-iter 10})
          param-grid (ml/param-grid
                      {hashing-tf {:num-features [10 100 1000]}
                       log-reg    {:reg-param [0.1 0.01] :max-iter [1 2 3]}})]
      (is (-> param-grid class .isArray))
      (is (= 18 (count param-grid)))
      (is (every? #(= (.size %) 3) param-grid))))
  (testing "takes params whose names have \"set\" in the middle"
    (is (= 2 (count (ml/param-grid
                     {(ml/random-forest-classifier {}) {:feature-subset-strategy ["auto" "sqrt"]}}))))
    (is (= 2 (count (ml/param-grid
                     {(ml/generalized-linear-regression {}) {:offset-col ["a" "b"]}}))))))

(deftest cross-validator-fitting-test
  (testing "should be able to replicate Spark example."
    (let [log-reg    (ml/logistic-regression {:max-iter 1})
          param-grid (ml/param-grid {log-reg {:reg-param [0.1]}})
          cv         (ml/cross-validator
                      {:estimator log-reg
                       :estimator-param-maps param-grid
                       :evaluator (ml/binary-classification-evaluator {})
                       :num-folds 2})
          model      (ml/fit (libsvm-df) cv)]
      (is (instance? LogisticRegressionModel (ml/best-model model))))))

(deftest tuning-results-test
  (let [log-reg    (ml/logistic-regression {:max-iter 1})
        param-grid (ml/param-grid {log-reg {:reg-param [0.1 0.01]}})
        options    {:estimator            log-reg
                    :estimator-param-maps param-grid
                    :evaluator            (ml/binary-classification-evaluator {})
                    :seed                 1}
        data       (g/limit (libsvm-df) 40)]
    (testing "a cross-validator's metric per param map, and its sub-models, when it keeps them"
      (let [model (ml/fit data (ml/cross-validator (assoc options :num-folds 2 :collect-sub-models true)))]
        (is (= 2 (count (ml/avg-metrics model))))
        (is (= [2 2] (map count (ml/sub-models model))))
        (is (every? #(instance? LogisticRegressionModel %) (flatten (ml/sub-models model))))))
    (testing "a train-validation split's"
      (let [model (ml/fit data (ml/train-validation-split (assoc options :train-ratio 0.6)))]
        (is (= 2 (count (ml/validation-metrics model))))
        (is (nil? (ml/sub-models model)))))))

(deftest cross-validator-test
  (testing "should be instantiatable"
    (is (instance? CrossValidator (ml/cross-validator {}))))
  (testing "should be able to replicate Spark example."
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
      (is (= 112233 (:seed cv-params)))
      (is (= 222 (:num-folds cv-params)))
      (is (= 101 (:parallelism cv-params))))))

(deftest train-validation-split-test
  (testing "should be instantiatable"
    (is (instance? TrainValidationSplit (ml/train-validation-split {}))))
  (testing "should be able to replicate Spark example."
    (let [split        (ml/train-validation-split
                        {:estimator (ml/logistic-regression {})
                         :evaluator (ml/binary-classification-evaluator {})
                         :estimator-param-maps (ml/param-grid {})
                         :seed 888
                         :train-ratio 0.6
                         :parallelism 777})
          split-params (ml/params split)]
      (is (= 0.6 (:train-ratio split-params)))
      (is (= 888 (:seed split-params)))
      (is (= 777 (:parallelism split-params))))))
