(ns zero-one.geni.window
  (:require
    [zero-one.geni.column :refer [->col-array]]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (org.apache.spark.sql.expressions Window)))

(defn- new-window []
  (Window/partitionBy (->col-array [])))

(defn- set-partition-by [window-spec & exprs]
  (.partitionBy window-spec (->col-array exprs)))

(defn- set-order-by [window-spec & exprs]
  (.orderBy window-spec (->col-array exprs)))

(defn- set-range-between [window-spec start end]
  (.rangeBetween window-spec start end))

(defn- set-rows-between [window-spec start end]
  (.rowsBetween window-spec start end))

(defn window [{:keys [partition-by order-by range-between rows-between]}]
  (let [partition-fn     (if partition-by
                           #(apply set-partition-by % (ensure-coll partition-by))
                           identity)
        order-fn         (if order-by
                           #(apply set-order-by % (ensure-coll order-by))
                           identity)
        range-between-fn (if range-between
                           #(apply set-range-between % (ensure-coll range-between))
                           identity)
        rows-between-fn  (if rows-between
                           #(apply set-rows-between % (ensure-coll rows-between))
                           identity)]
    (-> (new-window) partition-fn order-fn range-between-fn rows-between-fn)))

(defn over [column window-spec] (.over column window-spec))
