(ns zero-one.geni.ml-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g :refer [spark]]
    [zero-one.geni.dataset :as ds]
    [zero-one.geni.ml :as ml]))

(facts "On pipeline"
  (fact "should be able to fit the example stages"
    (let [dataset     (ds/table->dataset
                        @g/spark
                        [[0, "a b c d e spark", 1.0]
                         [1, "b d", 0.0]
                         [2, "spark f g h", 1.0],
                         [3, "hadoop mapreduce", 0.0]]
                        [:id :text :label])
          estimator   (ml/pipeline
                        (ml/tokenizer {:input-col "text"
                                       :output-col "words"})
                        (ml/hashing-tf {:num-features 1000
                                        :input-col "words"
                                        :output-col "features"})
                        (ml/logistic-regression {:max-iter 10
                                                 :reg-param 0.001}))
          transformer (ml/fit dataset estimator)
          dtypes      (-> dataset
                          (ml/transform transformer)
                          (g/select "probability" "prediction")
                          g/dtypes)]
      (dtypes "probability") => #(includes? % "Vector")
      (dtypes "prediction") => "DoubleType")))

(facts "On hypothesis testing"
  (let [dataset (ds/table->dataset
                   @spark
                   [[0.0 [0.5 10.0]]
                    [0.0 [1.5 20.0]]
                    [1.0 [1.5 30.0]]
                    [0.0 [3.5 30.0]]
                    [0.0 [3.5 40.0]]
                    [1.0 [3.5 40.0]]]
                   [:label :features])]
    (fact "able to do chi-squared test"
      (-> dataset
          (ml/chi-square-test "features" "label")
          g/first-vals
          first
          ml/vector->seq) => #(every? double? %))))

(facts "On correlation"
  (let [dataset     (ds/table->dataset
                       @spark
                       [[1.0 0.0 -2.0 0.0]
                        [4.0 5.0 0.0  3.0]
                        [6.0 7.0 0.0  8.0]
                        [9.0 0.0 1.0  0.0]]
                       [:a :b :c :d])
        v-assembler (ml/vector-assembler
                      {:input-cols ["a" "b" "c" "d"] :output-col "features"})
        features-df (-> dataset
                        (ml/transform v-assembler)
                        (g/select "features"))]
    (fact "should be able to make vectors"
      (-> features-df
          g/first-vals
          first
          ml/vector->seq) => [1.0 0.0 -2.0 0.0])
    (fact "should be able to calculate correlation"
      (let [corr-matrix (-> features-df
                            (ml/corr "features")
                            g/first-vals
                            first
                            ml/matrix->seqs)]
        (count corr-matrix) => 4
        (count (first corr-matrix)) => 4
        (every? double? (flatten corr-matrix)) => true))))

(fact "On param extraction"
  (ml/params (ml/logistic-regression {})) => {:max-iter 100,
                                              :family "auto",
                                              :tol 1.0E-6,
                                              :raw-prediction-col "rawPrediction",
                                              :elastic-net-param 0.0,
                                              :reg-param 0.0,
                                              :aggregation-depth 2,
                                              :threshold 0.5,
                                              :fit-intercept true,
                                              :label-col "label",
                                              :standardization true,
                                              :probability-col "probability",
                                              :prediction-col "prediction",
                                              :features-col "features"})
