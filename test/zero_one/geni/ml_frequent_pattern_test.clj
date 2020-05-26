(ns zero-one.geni.ml-frequent-pattern-test
  (:require
    [midje.sweet :refer [facts =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

(facts "On FP-Growth training"
  (let [dataset   (-> (g/table->dataset
                        spark
                        [["1 2 5"]
                         ["1 2 3 5"]
                         ["1 2"]]
                        [:items])
                      (g/with-column "items" (g/split "items" " ")))
        fp-growth (ml/fp-growth {:items-col      "items"
                                 :min-confidence 0.6
                                 :min-support    0.5})
        model     (ml/fit dataset fp-growth)]
    (g/column-names (ml/frequent-item-sets model)) => ["items" "freq"]
    (g/column-names (ml/association-rules model)) => ["antecedent"
                                                      "consequent"
                                                      "confidence"
                                                      "lift"]))
