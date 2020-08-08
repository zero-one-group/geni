(ns zero-one.geni.ml.recommendation
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.recommendation ALS)))

(defn als [params]
  (let [defaults {:implicit-prefs false,
                  :max-iter 10,
                  :intermediate-storage-level "MEMORY_AND_DISK",
                  :cold-start-strategy "nan",
                  :num-item-blocks 10,
                  :user-col "user",
                  :rank 10,
                  :nonnegative false,
                  :reg-param 0.1,
                  :seed 1994790107,
                  :final-storage-level "MEMORY_AND_DISK",
                  :checkpoint-interval 10,
                  :item-col "item",
                  :alpha 1.0,
                  :rating-col "rating",
                  :prediction-col "prediction",
                  :num-user-blocks 10}
        props    (merge defaults params)]
    (interop/instantiate ALS props)))
(def alternating-least-squares als)

(defn recommend-for-all-users [model num-items]
  (.recommendForAllUsers model num-items))

(defn recommend-for-all-items [model num-users]
  (.recommendForAllItems model num-users))

(defn recommend-for-user-subset [model users-df num-items]
  (.recommendForUserSubset model users-df num-items))

(defn recommend-for-item-subset [model items-df num-users]
  (.recommendForItemSubset model items-df num-users))

(defn recommend-items
  ([model num-items] (recommend-for-all-users model num-items))
  ([model users-df num-items] (recommend-for-user-subset model users-df num-items)))

(defn recommend-users
  ([model num-users] (recommend-for-all-items model num-users))
  ([model items-df num-users] (recommend-for-item-subset model items-df num-users)))

(defn item-factors [model] (.itemFactors model))

(defn user-factors [model] (.userFactors model))
