(ns zero-one.geni.dataset
  (:refer-clojure :exclude [distinct
                            drop
                            empty?
                            filter
                            group-by
                            remove
                            replace
                            sort
                            take])
  (:require
    [clojure.walk :refer [keywordize-keys]]
    [zero-one.geni.column :refer [->col-array ->column]]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [ensure-coll vector-of-numbers?]])
  (:import
    (org.apache.spark.sql Column RowFactory functions)
    (org.apache.spark.sql.types ArrayType DataTypes)
    (org.apache.spark.ml.linalg VectorUDT)))

;;;; Dataset Methods
;; Basic
(defn cache [dataframe] (.cache dataframe))

(defn columns [dataframe] (-> dataframe .columns seq))
(def column-names columns)

(defn dtypes [dataframe]
  (let [dtypes-as-tuples (-> dataframe .dtypes seq)]
    (->> dtypes-as-tuples
         (clojure.core/map interop/scala-tuple->vec)
         (into {})
         keywordize-keys)))

(defn explain
  ([dataframe] (.explain dataframe))
  ([dataframe extended] (.explain dataframe extended)))

(defn is-empty [dataframe] (.isEmpty dataframe))
(def empty? is-empty)

(defn is-local [dataframe] (.isLocal dataframe))
(def local? is-local)

(defn persist [dataframe] (.persist dataframe))

(defn print-schema [dataframe]
  (-> dataframe .schema .treeString println))

(defn rename-columns [dataframe rename-map]
  (reduce
    (fn [acc-df [old-name new-name]]
      (.withColumnRenamed acc-df old-name new-name))
    dataframe
    rename-map))

(defn show
  ([dataframe] (show dataframe {}))
  ([dataframe options]
   (let [{:keys [num-rows truncate vertical]
          :or   {num-rows 20
                 truncate 0
                 vertical false}} options]
     (-> dataframe (.showString num-rows truncate vertical) println))))

(defn show-vertical
  ([dataframe] (show dataframe {:vertical true}))
  ([dataframe options] (show dataframe (assoc options :vertical true))))

;; Typed Transformations
(defn distinct [dataframe] (.distinct dataframe))

(defn drop-duplicates [dataframe & col-names]
  (if (clojure.core/empty? col-names)
    (.dropDuplicates dataframe)
    (.dropDuplicates dataframe (into-array java.lang.String col-names))))

(defn except [dataframe other] (.except dataframe other))

(defn except-all [dataframe other] (.exceptAll dataframe other))

(defn filter [dataframe expr] (.filter dataframe expr))
(def where filter)

(defn intersect [dataframe other] (.intersect dataframe other))

(defn intersect-all [dataframe other] (.intersectAll dataframe other))

(defn join-with
  ([left right condition] (.joinWith left right condition))
  ([left right condition join-type] (.joinWith left right condition join-type)))

(defn limit [dataframe n-rows] (.limit dataframe n-rows))

(defn order-by [dataframe & exprs] (.orderBy dataframe (->col-array exprs)))
(def sort order-by)

(defn partitions [dataframe] (seq (.. dataframe rdd partitions)))

(defn random-split
  ([dataframe weights] (.randomSplit dataframe (double-array weights)))
  ([dataframe weights seed] (.randomSplit dataframe (double-array weights) seed)))

(defn remove [dataframe expr]
  (.filter dataframe (functions/not expr)))

(defn repartition [dataframe & args]
  (let [args          (clojure.core/flatten args)
        [head & tail] (clojure.core/flatten args)]
    (if (int? head)
      (.repartition dataframe head (->col-array tail))
      (.repartition dataframe (->col-array args)))))

(defn repartition-by-range [dataframe & args]
  (let [args          (clojure.core/flatten args)
        [head & tail] (clojure.core/flatten args)]
    (if (int? head)
      (.repartitionByRange dataframe head (->col-array tail))
      (.repartitionByRange dataframe (->col-array args)))))

(defn sample
  ([dataframe fraction] (.sample dataframe fraction))
  ([dataframe fraction with-replacement]
   (.sample dataframe with-replacement fraction)))

(defn sort-within-partitions [dataframe & exprs]
  (.sortWithinPartitions dataframe (->col-array exprs)))

(defn union [& dfs] (reduce #(.union %1 %2) dfs))

(defn union-by-name [& dfs] (reduce #(.unionByName %1 %2) dfs))

;; Untyped Transformations
(defn agg [dataframe & exprs]
  (let [[head & tail] (clojure.core/map ->column (clojure.core/flatten exprs))]
    (.agg dataframe head (into-array Column tail))))

(defn agg-all [dataframe agg-fn]
  (let [agg-cols (clojure.core/map agg-fn (column-names dataframe))]
    (apply agg dataframe agg-cols)))

(defn col-regex [dataframe col-name] (.colRegex dataframe col-name))

(defn cross-join [left right] (.crossJoin left right))

(defn cube [dataframe & exprs]
  (.cube dataframe (->col-array exprs)))

(defn drop [dataframe & col-names]
  (.drop dataframe (into-array java.lang.String col-names)))

(defn group-by [dataframe & exprs]
  (.groupBy dataframe (->col-array exprs)))

(defn join
  ([left right join-cols] (join left right join-cols "inner"))
  ([left right join-cols join-type]
   (let [join-cols (if (string? join-cols) [join-cols] join-cols)]
     (.join left right (interop/->scala-seq join-cols) join-type))))

(defn pivot
  ([grouped expr] (.pivot grouped (->column expr)))
  ([grouped expr values] (.pivot grouped (->column expr) (interop/->scala-seq values))))

(defn rollup [dataframe & exprs]
  (.rollup dataframe (->col-array exprs)))

(defn select [dataframe & exprs] (.select dataframe (->col-array exprs)))

(defn select-expr [dataframe & exprs]
  (.selectExpr dataframe (into-array java.lang.String exprs)))

(defn with-column [dataframe col-name expr]
  (.withColumn dataframe col-name (->column expr)))

(defn with-column-renamed [dataframe old-name new-name]
  (.withColumnRenamed dataframe old-name new-name))

;; Ungrouped
(defn spark-session [dataframe] (.sparkSession dataframe))

(defn sql-context [dataframe] (.sqlContext dataframe))

;; Stat Functions
(defn approx-quantile [dataframe col-or-cols probs rel-error]
  (let [seq-col     (coll? col-or-cols)
        col-or-cols (if seq-col
                      (into-array java.lang.String col-or-cols)
                      col-or-cols)
        quantiles   (-> dataframe
                        .stat
                        (.approxQuantile col-or-cols (double-array probs) rel-error))]
    (if seq-col
      (clojure.core/map seq quantiles)
      (seq quantiles))))

;; Actions
(defn collect [dataframe]
  (->> dataframe .collect seq (clojure.core/map interop/->clojure)))
(defn take [dataframe n-rows] (-> dataframe (limit n-rows) collect))

(defn describe [dataframe & column-names]
  (.describe dataframe (into-array java.lang.String column-names)))
(defn summary [dataframe & stat-names]
  (.summary dataframe (into-array java.lang.String stat-names)))

;;;; Actions for Rows
(defn collect-vals [dataframe]
  (clojure.core/map vals (collect dataframe)))
(defn collect-col [dataframe col-name]
  (clojure.core/map (keyword col-name) (-> dataframe (select col-name) collect)))
(defn take-vals [dataframe n-rows] (-> dataframe (limit n-rows) collect-vals))
(defn first-vals [dataframe] (-> dataframe (take-vals 1) clojure.core/first))

;; NA Functions
(defn drop-na
  ([dataframe]
   (-> dataframe .na .drop))
  ([dataframe min-non-nulls-or-cols]
   (if (coll? min-non-nulls-or-cols)
     (-> dataframe .na (.drop (interop/->scala-seq min-non-nulls-or-cols)))
     (-> dataframe .na (.drop min-non-nulls-or-cols))))
  ([dataframe min-non-nulls cols]
   (-> dataframe .na (.drop min-non-nulls (interop/->scala-seq cols)))))

(defn fill-na
  ([dataframe value]
   (-> dataframe .na (.fill value)))
  ([dataframe value cols]
   (-> dataframe .na (.fill value (interop/->scala-seq cols)))))

(defn replace [dataframe cols replacement]
  (let [cols (ensure-coll cols)]
    (-> dataframe
        .na
        (.replace (into-array java.lang.String cols)
                  (java.util.HashMap. replacement)))))

;;;; Dataset Creation
(defn ->row [coll]
  (RowFactory/create (into-array Object (map interop/->scala-coll coll))))

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

(def java-type->spark-type
  {java.lang.Boolean             DataTypes/BooleanType
   java.lang.Byte                DataTypes/ByteType
   java.lang.Double              DataTypes/DoubleType
   java.lang.Float               DataTypes/FloatType
   java.lang.Integer             DataTypes/IntegerType
   java.lang.Long                DataTypes/LongType
   java.lang.Short               DataTypes/ShortType
   java.lang.String              DataTypes/StringType
   java.sql.Timestamp            DataTypes/TimestampType
   java.util.Date                DataTypes/DateType
   nil                           DataTypes/NullType})

(defn infer-spark-type [value]
  (cond
    (vector-of-numbers? value) (VectorUDT.)
    (coll? value) (ArrayType. (infer-spark-type (first value)) true)
    :else (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn first-non-nil [values]
  (first (clojure.core/filter clojure.core/identity values)))

(defn transpose [xs]
  (apply map list xs))

(defn table->dataset [spark table col-names]
  (let [col-names (map name col-names)
        values    (map first-non-nil (transpose table))
        rows      (->java-list (map ->row table))
        schema    (infer-schema col-names values)]
    (.createDataFrame spark rows schema)))

(defn map->dataset [spark map-of-values]
  (let [table     (transpose (vals map-of-values))
        col-names (keys map-of-values)]
    (table->dataset spark table col-names)))

(defn conj-record [map-of-values record]
  (let [col-names (keys map-of-values)]
    (reduce
      (fn [acc-map col-name]
        (update acc-map col-name #(conj % (get record col-name))))
      map-of-values
      col-names)))

(defn records->dataset [spark records]
  (let [col-names     (-> (map keys records) flatten clojure.core/distinct)
        map-of-values (reduce
                        conj-record
                        (zipmap col-names (repeat []))
                        records)]
    (map->dataset spark map-of-values)))
