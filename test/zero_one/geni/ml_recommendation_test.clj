(ns zero-one.geni.ml-recommendation-test
  (:require
    [midje.sweet :refer [facts =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [ratings-df]]))

(facts "On recommendation" :slow
  (let [estimator   (ml/als {:max-iter   1
                             :reg-param  0.01
                             :user-col   "user-id"
                             :item-col   "movie-id"
                             :rating-col "rating"})
        model       (ml/fit ratings-df estimator)
        predictions (do
                      (.setColdStartStrategy model "drop")
                      (ml/transform ratings-df model))
        evaluator   (ml/regression-evaluator {:metric-name    "rmse"
                                              :label-col      "rating"
                                              :prediction-col "prediction"})
        rmse        (ml/evaluate predictions evaluator)
        some-users  (-> ratings-df (g/select "user-id") g/distinct (g/limit 3))
        some-items  (-> ratings-df (g/select "movie-id") g/distinct (g/limit 5))]
    rmse => #(<= % 0.9)
    (let [recommendations (ml/recommend-users model 6)]
      (-> recommendations g/collect-vals flatten) => #(every? number? %)
      (g/column-names recommendations) => ["movie-id" "recommendations"]
      (g/count recommendations) => 100)
    (let [recommendations (ml/recommend-users model some-items 7)]
      (-> recommendations g/collect-vals flatten) => #(every? number? %)
      (g/column-names recommendations) => ["movie-id" "recommendations"]
      (g/count recommendations) => 5)
    (let [recommendations (ml/recommend-items model 8)]
      (-> recommendations g/collect-vals flatten) => #(every? number? %)
      (g/column-names recommendations) => ["user-id" "recommendations"]
      (g/count recommendations) => 30)
    (let [recommendations (ml/recommend-items model some-users 9)]
      (-> recommendations g/collect-vals flatten) => #(every? number? %)
      (g/column-names recommendations) => ["user-id" "recommendations"]
      (g/count recommendations) => 3)))
