(ns zero-one.geni.numpy-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [spark df-20]]))

(defn descriptive-stats [col]
  (-> (g/table->dataset @spark (mapv vector (range 200)) [:idx])
      (g/with-column :x col)
      (g/agg {:min  (g/min :x)
              :mean (g/mean :x)
              :std  (g/stddev :x)
              :max  (g/max :x)})
      g/first))

(deftest ^:slow random-exp-test
  (let [actual (descriptive-stats (g/random-exp))]
    (is (and (< 0.5 (:mean actual) 1.5)
             (< 0.5 (:std actual) 1.5))))
  (let [actual (descriptive-stats (g/random-exp 5))]
    (is (and (< 0.1 (:mean actual) 0.3)
             (< 0.1 (:std actual) 0.3)))))

(deftest ^:slow random-norm-test
  (let [actual (descriptive-stats (g/random-norm))]
    (is (and (< -0.3 (:mean actual) 0.3)
             (< -0.8 (:std actual) 1.2))))
  (let [actual (descriptive-stats (g/random-norm -3 2))]
    (is (and (< -4.0 (:mean actual) -2.0)
             (< 1.5 (:std actual) 2.5)))))

(deftest ^:slow random-int-test
  (let [actual (descriptive-stats (g/random-int))]
    (is (and (pos? (:max actual))
             (pos? (:min actual))
             (integer? (:max actual))
             (integer? (:min actual)))))
  (let [actual (descriptive-stats (g/random-int 1 13))]
    (is (and (= (:max actual) 12)
             (= (:min actual) 1)
             (integer? (:max actual))
             (integer? (:min actual)))))
  (let [actual (descriptive-stats (g/random-int -5 -2))]
    (is (and (= (:max actual) -3)
             (= (:min actual) -5)
             (integer? (:max actual))
             (integer? (:min actual))))))

(deftest ^:slow random-uniform-test
  (let [actual (descriptive-stats (g/random-uniform))]
    (is (and (< 0.95 (:max actual) 1.00)
             (< 0.00 (:min actual) 0.05)
             (double? (:max actual))
             (double? (:min actual)))))
  (let [actual (descriptive-stats (g/random-uniform -0.5 -1.0))]
    (is (and (< -0.55 (:max actual) -0.50)
             (< -1.00 (:min actual) -0.95)
             (double? (:max actual))
             (double? (:min actual))))))

(deftest ^:slow random-choice-test
  (is (= #{"abc" "def" "ghi"}
         (-> (g/table->dataset @spark (mapv vector (range 100)) [:idx])
             (g/with-column :rand-choice (g/random-choice [(g/lit "abc")
                                                           (g/lit "def")
                                                           (g/lit "ghi")]))
             (g/collect-col :rand-choice)
             set)))
  (is (= (mapv :rand-choice (-> (g/table->dataset @spark (mapv vector (range 2000)) [:idx])
                                (g/with-column :rand-choice (g/random-choice [0 1 2 3] [0.5 0.3 0.15 0.05]))
                                (g/select :rand-choice)
                                g/value-counts
                                g/collect)) [0 1 2 3]))
  (is (thrown? AssertionError (g/random-choice [0] [2.0])))
  (is (thrown? AssertionError (g/random-choice [] [1.0])))
  (is (thrown? AssertionError (g/random-choice [0 1] [-1.0 2.0]))))

(deftest ^:slow clip-test
  (let [xs (-> (df-20)
               (g/select (g/clip :Price 9e5 1.1e6))
               g/collect-vals
               flatten)]
    (is (every? #(<= 9e5 % 1.1e6) xs))))
