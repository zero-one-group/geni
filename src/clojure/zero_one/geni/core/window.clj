(ns zero-one.geni.core.window
  (:require
   [zero-one.geni.core.column :refer [->col-array]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.utils :refer [ensure-coll]])
  (:import
   (org.apache.spark.sql.expressions Window)))

(defn window [{:keys [partition-by order-by range-between rows-between]}]
  (-> (Window/partitionBy (->col-array []))
      (cond-> partition-by (.partitionBy (->col-array (ensure-coll partition-by))))
      (cond-> order-by (.orderBy (->col-array (ensure-coll order-by))))
      (cond-> range-between (.rangeBetween (:start range-between) (:end range-between)))
      (cond-> rows-between (.rowsBetween (:start rows-between) (:end rows-between)))))

(defn over [column window-spec] (.over column window-spec))

(def unbounded-following (Window/unboundedFollowing))

(def unbounded-preceding (Window/unboundedPreceding))

(defn windowed
  "Shortcut to create WindowSpec that takes a map as the argument.

  Expected keys:  [:partition-by :order-by :range-between :rows-between]"
  [options]
  (over (:window-col options)
        (window (select-keys options [:partition-by
                                      :order-by
                                      :range-between
                                      :rows-between]))))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.core.window
 [(-> docs/spark-docs :methods :core :window)
  (-> docs/spark-docs :classes :core :window)])

(docs/add-doc!
 (var over)
 (-> docs/spark-docs :methods :core :column :over))
