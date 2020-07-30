(ns zero-one.geni.polymorphic
  (:refer-clojure :exclude [alias
                            assoc
                            count
                            dissoc
                            filter
                            first
                            last
                            max
                            min
                            shuffle])
  (:require
    [clojure.string]
    [zero-one.geni.column :refer [->col-array ->column]]
    [zero-one.geni.dataset]
    [zero-one.geni.dataset-creation]
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.sql :as sql]
    [zero-one.geni.utils :refer [->string-map arg-count ensure-coll]])
  (:import
    (org.apache.spark.ml.stat Correlation)
    (org.apache.spark.sql Dataset
                          RelationalGroupedDataset
                          functions)))

(def default-spark zero-one.geni.defaults/spark)

(defmulti as (fn [head & _] (class head)))
(defmethod as :default [expr new-name] (.as (->column expr) (name new-name)))
(defmethod as Dataset [dataframe new-name] (.as dataframe (name new-name)))
(def alias as)

(defmulti count class)
(defmethod count :default [expr] (functions/count (->column expr)))
(defmethod count Dataset [dataset] (.count dataset))
(defmethod count RelationalGroupedDataset [grouped] (.count grouped))

(defmulti explain (fn [head & _] (class head)))
(defmethod explain :default [expr extended] (.explain (->column expr) extended))
(defmethod explain Dataset
  ([dataset] (.explain dataset))
  ([dataset extended] (.explain dataset extended)))

(defmulti mean (fn [head & _] (class head)))
(defmethod mean :default [expr & _] (functions/mean (->column expr)))
(defmethod mean RelationalGroupedDataset [grouped & col-names]
  (.mean grouped (interop/->scala-seq (clojure.core/map name col-names))))
(def avg mean)

(defmulti max (fn [head & _] (class head)))
(defmethod max :default [expr] (functions/max (->column expr)))
(defmethod max RelationalGroupedDataset [grouped & col-names]
  (.max grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti min (fn [head & _] (class head)))
(defmethod min :default [expr] (functions/min (->column expr)))
(defmethod min RelationalGroupedDataset [grouped & col-names]
  (.min grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti sum (fn [head & _] (class head)))
(defmethod sum :default [expr] (functions/sum (->column expr)))
(defmethod sum RelationalGroupedDataset [grouped & col-names]
  (.sum grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti coalesce (fn [head & _] (class head)))
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions))
(defmethod coalesce :default [& exprs]
  (functions/coalesce (->col-array exprs)))

(defmulti shuffle class)
(defmethod shuffle :default [expr]
  (functions/shuffle (->column expr)))
(defmethod shuffle Dataset [dataframe]
  (zero-one.geni.dataset/sort dataframe (functions/randn)))

(defmulti first class)
(defmethod first Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))

(defmulti last class)
(defmethod last Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/tail 1) clojure.core/first))
(defmethod last :default [expr] (functions/last (->column expr)))

(defmulti filter (fn [head & _] (class head)))
(defmethod filter Dataset [dataframe expr]
  (.filter dataframe (.cast (->column expr) "boolean")))
(defmethod filter :default [expr predicate]
  (let [scala-predicate (if (= (arg-count predicate) 2)
                          (interop/->scala-function2 predicate)
                          (interop/->scala-function1 predicate))]
    (functions/filter (->column expr) scala-predicate)))
(def where filter)

(defmulti to-json (fn [head & _] (class head)))
(defmethod to-json Dataset [dataframe] (.toJSON dataframe))
(defmethod to-json :default
  ([expr] (functions/to_json (->column expr) {}))
  ([expr options]
   (functions/to_json (->column expr) (->string-map options))))

(defmulti to-df (fn [head & _] (class head)))
(defmethod to-df :default
  ([table col-names]
   (to-df @default-spark table col-names))
  ([spark table col-names]
   (zero-one.geni.dataset-creation/table->dataset spark table col-names)))
(defmethod to-df Dataset
  ([dataframe] (.toDF dataframe))
  ([dataframe & col-names]
   (.toDF dataframe (->> col-names
                         (mapcat ensure-coll)
                         (map name)
                         interop/->scala-seq))))

(defmulti corr (fn [head & _] (class head)))
(defmethod corr :default [l-expr r-expr]
  (functions/corr (->column l-expr) (->column r-expr)))
(defmethod corr Dataset
  ([dataframe col-name]
   (Correlation/corr dataframe (name col-name)))
  ([dataframe col-name1 col-name2]
   (-> dataframe .stat (.corr (name col-name1) (name col-name2))))
  ([dataframe col-name1 col-name2 method]
   (-> dataframe .stat (.corr (name col-name1) (name col-name2) method))))

;; Tech ML
;; TODO: add update
(defmulti assoc (fn [head & _] (class head)))
(defmethod assoc :default
  ([expr k v] (sql/map-concat expr (sql/map k v)))
  ([expr k v & kvs]
   (if (even? (clojure.core/count kvs))
     (let [assoced (assoc expr k v)]
       (reduce (fn [m [k v]] (assoc m k v)) assoced (partition 2 kvs)))
     (throw (IllegalArgumentException. (str "assoc expects even number of arguments "
                                            "after map/vector, found odd number"))))))
(defmethod assoc Dataset
  ([dataframe k v] (.withColumn dataframe (name k) (->column v)))
  ([dataframe k v & kvs]
   (if (even? (clojure.core/count kvs))
     (let [assoced (assoc dataframe k v)]
       (reduce (fn [m [k v]] (assoc m k v)) assoced (partition 2 kvs)))
     (throw (IllegalArgumentException. (str "assoc expects even number of arguments "
                                            "after map/vector, found odd number"))))))

(defmulti dissoc (fn [head & _] (class head)))
(defmethod dissoc :default [expr & ks]
  (sql/map-filter
    expr
    (fn [k _] (functions/not (.isin k (interop/->scala-seq ks))))))
(defmethod dissoc Dataset [dataframe & col-names]
  (apply zero-one.geni.dataset/drop dataframe col-names))

;; Pandas
(defmulti quantile (fn [head & _] (class head)))
(defmethod quantile :default [col-name percs]
  (let [percs-str   (if (coll? percs)
                      (str "array(" (clojure.string/join ", " (map str percs)) ")")
                      (str percs))
        median-expr (str "percentile_approx(" (name col-name) ", " percs-str ")")
        median-name (str "quantile(" (name col-name) ", " percs-str ")")]
    (as (sql/expr median-expr) median-name)))
(defmethod quantile RelationalGroupedDataset [grouped percs col-names]
  (zero-one.geni.dataset/agg grouped (map #(quantile % percs) col-names)))

(defmulti iqr (fn [head & _] (class head)))
(defmethod iqr :default [col-name]
  (as (.minus (quantile col-name 0.75) (quantile col-name 0.25))
      (str "iqr(" (name col-name) ")")))
(defmethod iqr RelationalGroupedDataset [grouped & col-names]
  (zero-one.geni.dataset/agg grouped (->> col-names
                                          (mapcat ensure-coll)
                                          (map iqr))))
(def interquartile-range iqr)

(defmulti median (fn [head & _] (class head)))
(defmethod median :default [col-name]
  (let [median-expr (str "percentile_approx(" (name col-name) " , 0.5)")
        median-name (str "median(" (name col-name) ")")]
    (as (sql/expr median-expr) median-name)))
(defmethod median RelationalGroupedDataset [grouped & col-names]
  (zero-one.geni.dataset/agg grouped (map median col-names)))
