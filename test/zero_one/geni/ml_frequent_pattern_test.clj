(ns ^:classic zero-one.geni.ml-frequent-pattern-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [spark]]))

(deftest prefix-span-training-test
  (let [dataset     (-> (g/table->dataset
                         @spark
                         [[['(1 2) '(3)]]
                          [['(1) '(3 2) '(1 2)]]
                          [['(1 2) '(5)]]]
                         [:sequence])
                        g/cache)
        prefix-span (ml/prefix-span {:min-support 0.5
                                     :max-pattern-length 5
                                     :max-local-proj-db-size 32000000})]
    (is (= ["sequence" "freq"]
           (-> dataset
               (ml/find-patterns prefix-span)
               g/column-names)))))

(deftest fp-growth-training-test
  (let [dataset   (-> (g/table->dataset
                       @spark
                       [[["1" "2" "5"]]
                        [["1" "2" "3" "5"]]
                        [["1" "2"]]]
                       [:items])
                      g/cache)
        fp-growth (ml/fp-growth {:items-col      :items
                                 :min-confidence 0.6
                                 :min-support    0.5})
        model     (ml/fit dataset fp-growth)]
    (is (= ["items" "freq"] (g/column-names (ml/frequent-item-sets model))))
    (is (= ["antecedent"
            "consequent"
            "confidence"
            "lift"
            "support"]
           (g/column-names (ml/association-rules model))))))
