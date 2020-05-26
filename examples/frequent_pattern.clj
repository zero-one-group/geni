(ns examples.classification
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

(def dataset
  (-> (g/table->dataset
        spark
        [["1 2 5"]
         ["1 2 3 5"]
         ["1 2"]]
        [:items])
      (g/with-column "items" (g/split "items" " "))))

(def model
  (ml/fit
    dataset
    (ml/fp-growth {:items-col      "items"
                   :min-confidence 0.6
                   :min-support    0.5})))


(g/show (ml/frequent-item-sets model))
(g/show (ml/association-rules model))

