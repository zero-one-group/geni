(ns zero-one.geni.core.polymorphic
  (:refer-clojure :exclude [alias
                            assoc
                            count
                            dissoc
                            filter
                            first
                            last
                            max
                            min
                            shuffle
                            update])
  (:require
   [clojure.string]
   [potemkin :refer [import-fn]]
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.core.dataset :as dataset]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.core.functions :as sql]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [->string-map arg-count ensure-coll]])
  (:import
   (org.apache.spark.ml.stat Correlation)
   (org.apache.spark.sql Dataset
                         RelationalGroupedDataset
                         functions)))

(defmulti as
  "Column: Gives the column an alias.

   Dataset: Returns a new Dataset with an alias set."
  (fn [head & _] (class head)))
(defmethod as :default [expr new-name] (.as (->column expr) (name new-name)))
(defmethod as Dataset [dataframe new-name] (.as dataframe (name new-name)))

(defmulti count
  "Column: Aggregate function: returns the number of items in a group.

   Dataset: Returns the number of rows in the Dataset.

   RelationalGroupedDataset: Count the number of rows for each group."
  class)
(defmethod count :default [expr] (functions/count (->column expr)))
(defmethod count Dataset [dataset] (.count dataset))
(defmethod count RelationalGroupedDataset [grouped] (.count grouped))

(defmulti explain
  "Column: Prints the expression to the console for debugging purposes.

   Dataset: Prints the physical plan to the console for debugging purposes."
  (fn [head & _] (class head)))
(defmethod explain :default [expr extended] (.explain (->column expr) extended))
(defmethod explain Dataset
  ([dataset] (.explain dataset))
  ([dataset extended] (.explain dataset extended)))

(defmulti mean
  "Column: Aggregate function: returns the average of the values in a group.

   RelationalGroupedDataset: Compute the average value for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod mean :default [expr & _] (functions/mean (->column expr)))
(defmethod mean RelationalGroupedDataset [grouped & col-names]
  (.mean grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti max
  "Column: Aggregate function: returns the maximum value of the column in a group.

   RelationalGroupedDataset: Compute the max value for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod max :default [expr] (functions/max (->column expr)))
(defmethod max RelationalGroupedDataset [grouped & col-names]
  (.max grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti min
  "Column: Aggregate function: returns the minimum value of the column in a group.

   RelationalGroupedDataset: Compute the min value for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod min :default [expr] (functions/min (->column expr)))
(defmethod min RelationalGroupedDataset [grouped & col-names]
  (.min grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti sum
  "Column: Aggregate function: returns the sum of all values in the given column.

   RelationalGroupedDataset: Compute the sum for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod sum :default [expr] (functions/sum (->column expr)))
(defmethod sum RelationalGroupedDataset [grouped & col-names]
  (.sum grouped (interop/->scala-seq (clojure.core/map name col-names))))

(defmulti coalesce
  "Column: Returns the first column that is not null, or null if all inputs are null.

   Dataset: Returns a new Dataset that has exactly numPartitions partitions, when the fewer partitions are requested."
  (fn [head & _] (class head)))
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions))
(defmethod coalesce :default [& exprs]
  (functions/coalesce (->col-array exprs)))

(defmulti shuffle
  "Column: Returns a random permutation of the given array.

   Dataset: Shuffles the rows of the Dataset."
  class)
(defmethod shuffle :default [expr]
  (functions/shuffle (->column expr)))
(defmethod shuffle Dataset [dataframe]
  (dataset/sort dataframe (functions/randn)))

(defmulti first
  "Column: Aggregate function: returns the first value of a column in a group.

   Dataset: Returns the first row."
  class)
(defmethod first Dataset [dataframe]
  (-> dataframe (dataset/take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))

(defmulti last
  "Column: Aggregate function: returns the last value of the column in a group.

   Dataset: Returns the last row."
  class)
(defmethod last Dataset [dataframe]
  (-> dataframe (dataset/tail 1) clojure.core/first))
(defmethod last :default [expr] (functions/last (->column expr)))

(defmulti filter
  "Column: Returns an array of elements for which a predicate holds in a given array.

   Dataset: Filters rows using the given condition."
  (fn [head & _] (class head)))
(defmethod filter Dataset [dataframe expr]
  (.filter dataframe (.cast (->column expr) "boolean")))
(defmethod filter :default [expr predicate]
  (let [scala-predicate (if (= (arg-count predicate) 2)
                          (interop/->scala-function2 predicate)
                          (interop/->scala-function1 predicate))]
    (functions/filter (->column expr) scala-predicate)))

(defmulti to-json
  "Column: Converts a column containing a StructType, ArrayType or a MapType into a JSON string with the specified schema.

   Dataset: Returns the content of the Dataset as a Dataset of JSON strings."
  (fn [head & _] (class head)))
(defmethod to-json Dataset [dataframe] (.toJSON dataframe))
(defmethod to-json :default
  ([expr] (functions/to_json (->column expr) {}))
  ([expr options]
   (functions/to_json (->column expr) (->string-map options))))

(defmulti to-df
  "Collection: alias for `table->dataset`.

   Dataset: Converts this strongly typed collection of data to generic DataFrame with columns renamed."
  (fn [head & _] (class head)))
(defmethod to-df :default
  ([table col-names]
   (to-df @defaults/spark table col-names))
  ([spark table col-names]
   (dataset-creation/table->dataset spark table col-names)))
(defmethod to-df Dataset
  ([dataframe] (.toDF dataframe))
  ([dataframe & col-names]
   (.toDF dataframe (->> col-names
                         (mapcat ensure-coll)
                         (map name)
                         interop/->scala-seq))))

(defmulti corr
  "Column: Aggregate function: returns the Pearson Correlation Coefficient for two columns.

   Datasate: Calculates the Pearson Correlation Coefficient of two columns of a DataFrame."
  (fn [head & _] (class head)))
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
(defmulti assoc
  "Column: variadic version of `map-concat`.

   Dataset: variadic version of `with-column`."
  (fn [head & _] (class head)))
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

(defmulti dissoc
  "Column: Returns a map whose key is not in `ks`.

   Dataset: variadic version of `drop`."
  (fn [head & _] (class head)))
(defmethod dissoc :default [expr & ks]
  (sql/map-filter
   expr
   (fn [k _] (functions/not (.isin k (interop/->scala-seq ks))))))
(defmethod dissoc Dataset [dataframe & col-names]
  (apply dataset/drop dataframe col-names))

(defmulti update'
  "Column: `transform-values` with Clojure's `assoc` signature.

   Dataset: `with-column` with Clojure's `assoc` signature."
  (fn [head & _] (class head)))
(defmethod update' :default [expr k f & args]
  (sql/transform-values
   expr
   (fn [k' v] (sql/when (.equalTo (->column k') (->column k))
                (apply f v args)
                v))))
(defmethod update' Dataset [dataframe k f & args]
  (dataset/with-column dataframe k (apply f k args)))

;; Pandas
(defmulti quantile
  "Column: Aggregate function: returns the quantile of the values in a group.

   RelationalGroupedDataset: Compute the quantile for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod quantile :default [col-name percs]
  (let [percs-str     (if (coll? percs)
                        (str "array(" (clojure.string/join ", " (map str percs)) ")")
                        (str percs))
        quantile-expr (str "percentile_approx(" (name col-name) ", " percs-str ")")
        quantile-name (str "quantile(" (name col-name) ", " percs-str ")")]
    (as (sql/expr quantile-expr) quantile-name)))
(defmethod quantile RelationalGroupedDataset [grouped percs col-names]
  (dataset/agg grouped (map #(quantile % percs) col-names)))

(defmulti iqr
  "Column: Aggregate function: returns the inter-quartile range of the values in a group.

   RelationalGroupedDataset: Compute the inter-quartile range for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod iqr :default [col-name]
  (as (.minus (quantile col-name 0.75) (quantile col-name 0.25))
      (str "iqr(" (name col-name) ")")))
(defmethod iqr RelationalGroupedDataset [grouped & col-names]
  (dataset/agg grouped (->> col-names
                            (mapcat ensure-coll)
                            (map iqr))))

(defmulti median
  "Column: Aggregate function: returns the median range of the values in a group.

   RelationalGroupedDataset: Compute the median range for each numeric columns for each group."
  (fn [head & _] (class head)))
(defmethod median :default [col-name]
  (let [median-expr (str "percentile_approx(" (name col-name) " , 0.5)")
        median-name (str "median(" (name col-name) ")")]
    (as (sql/expr median-expr) median-name)))
(defmethod median RelationalGroupedDataset [grouped & col-names]
  (dataset/agg grouped (map median col-names)))

;; Aliases
(import-fn as alias)
(import-fn filter where)
(import-fn iqr interquartile-range)
(import-fn mean avg)
(import-fn update' update)

(comment
  (require '[zero-one.geni.docs :as docs])
  (docs/docless-vars *ns*))
