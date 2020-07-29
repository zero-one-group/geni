(ns examples.recommendation
  (:require
    [clojure.string]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

(defonce ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       clojure.string/split-lines
       (map #(clojure.string/split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (g/records->dataset)))

(def model
  (ml/fit ratings-df (ml/als {:max-iter   5
                              :reg-param  0.01
                              :user-col   :user-id
                              :item-col   :movie-id
                              :rating-col :rating})))

(.setColdStartStrategy model "drop")
(def predictions
  (ml/transform ratings-df model))

(def evaluator
  (ml/regression-evaluator {:metric-name    "rmse"
                            :label-col      :rating
                            :prediction-col :prediction}))

(println "Root-mean-square error:" (ml/evaluate predictions evaluator))
(-> (ml/recommend-users model 3)
    (g/limit 5)
    g/show)
(-> (ml/recommend-items model 3)
    (g/limit 5)
    g/show)
