(ns zero-one.geni.ml-recommendation-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [ratings-df]]))

(deftest ^:slow recommendation-test
  (let [estimator   (ml/als {:max-iter        1
                             :num-user-blocks 1
                             :num-item-blocks 1
                             :reg-param       0.01
                             :user-col   :user-id
                             :item-col   :movie-id
                             :rating-col :rating})
        model       (ml/fit (ratings-df) estimator)
        predictions (do
                      (.setColdStartStrategy model "drop")
                      (ml/transform (ratings-df) model))
        evaluator   (ml/regression-evaluator {:metric-name    "rmse"
                                              :label-col      :rating
                                              :prediction-col :prediction})
        rmse        (ml/evaluate predictions evaluator)
        some-users  (-> (ratings-df) (g/select :user-id) g/distinct (g/limit 3))
        some-items  (-> (ratings-df) (g/select :movie-id) g/distinct (g/limit 5))]
    (is (<= rmse 0.9))
    (is (= 100 (g/count (ml/item-factors model))))
    (is (= 30 (g/count (ml/user-factors model))))
    (let [recommendations (ml/recommend-users model 6)]
      (let [actual (-> recommendations g/collect)]
        (is (and (every? number? (map :movie-id actual))
                 (every? map? (mapcat :recommendations actual)))))
      (is (= [:movie-id :recommendations] (g/columns recommendations)))
      (is (= 100 (g/count recommendations))))
    (let [recommendations (ml/recommend-users model some-items 7)]
      (let [actual (-> recommendations g/collect)]
        (is (and (every? number? (map :movie-id actual))
                 (every? map? (mapcat :recommendations actual)))))
      (is (= [:movie-id :recommendations] (g/columns recommendations)))
      (is (= 5 (g/count recommendations))))
    (let [recommendations (ml/recommend-items model 8)]
      (let [actual (-> recommendations g/collect)]
        (is (and (every? number? (map :user-id actual))
                 (every? map? (mapcat :recommendations actual)))))
      (is (= [:user-id :recommendations] (g/columns recommendations)))
      (is (= 30 (g/count recommendations))))
    (let [recommendations (ml/recommend-items model some-users 9)]
      (let [actual (-> recommendations g/collect)]
        (is (and (every? number? (map :user-id actual))
                 (every? map? (mapcat :recommendations actual)))))
      (is (= [:user-id :recommendations] (g/columns recommendations)))
      (is (= 3 (g/count recommendations))))))
