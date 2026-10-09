(ns zero-one.geni.ml.recommendation
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]]))

(interop/def-stages org.apache.spark.ml.recommendation
  [als ALS])

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

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.recommendation
 [(-> docs/spark-docs :classes :ml :recommendation)
  (-> docs/spark-docs :methods :ml :models :als)])

(docs/add-doc!
 (var recommend-users)
 (-> docs/spark-docs :methods :ml :models :als :recommend-for-all-items))

(docs/add-doc!
 (var recommend-items)
 (-> docs/spark-docs :methods :ml :models :als :recommend-for-all-users))

;; Aliases
(import-fn als alternating-least-squares)
