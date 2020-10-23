(ns zero-one.geni.ml-frequent-pattern-test
  (:require
   [midje.sweet :refer [facts =>]]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [spark]]))

(facts "On Prefix-Span training" :slow
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
    (-> dataset
        (ml/find-patterns prefix-span)
        g/column-names) => ["sequence" "freq"]))

(facts "On FP-Growth training" :slow
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
    (g/column-names (ml/frequent-item-sets model)) => ["items" "freq"]
    (g/column-names (ml/association-rules model)) => ["antecedent"
                                                      "consequent"
                                                      "confidence"
                                                      "lift"]))
