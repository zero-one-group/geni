(ns zero-one.geni.core
  (:refer-clojure :exclude [*
                            +
                            -
                            /
                            <
                            <=
                            >
                            >=
                            alias
                            cast
                            concat
                            count
                            distinct
                            drop
                            empty?
                            filter
                            first
                            flatten
                            group-by
                            hash
                            last
                            map
                            max
                            min
                            mod
                            not
                            partition-by
                            rand
                            reverse
                            second
                            shuffle
                            take
                            when])
  (:require
    [clojure.walk :refer [keywordize-keys]]
    [potemkin :refer [import-vars]]
    [zero-one.geni.column]
    [zero-one.geni.dataset]
    [zero-one.geni.data-sources]
    [zero-one.geni.sql]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (org.apache.spark.sql Column Dataset functions)
    (org.apache.spark.sql SparkSession)
    (org.apache.spark.sql.expressions Window)))

(import-vars
  [zero-one.geni.column
   col
   ->col-array
   ->column])

(import-vars
  [zero-one.geni.sql
   &&
   *
   +
   -
   ->date-col
   /
   <
   <=
   ===
   >
   >=
   abs
   acos
   add-months
   alias
   approx-count-distinct
   array
   array-contains
   array-distinct
   array-except
   array-intersect
   array-join
   array-max
   array-min
   array-position
   array-remove
   array-repeat
   array-sort
   array-union
   arrays-overlap
   arrays-zip
   as
   asc
   asin
   atan
   avg
   between
   broadcast
   cast
   ceil
   collect-list
   collect-set
   concat
   contains
   corr
   cos
   cosh
   count-distinct
   covar
   covar-pop
   covar-samp
   cume-dist
   current-date
   current-timestamp
   date-add
   date-diff
   date-format
   date-sub
   datediff
   day-of-month
   day-of-week
   day-of-year
   dense-rank
   desc
   element-at
   ends-with
   exp
   explode
   expr
   flatten
   floor
   format-number
   format-string
   hash
   hour
   kurtosis
   lag
   last
   last-day
   lead
   like
   lit
   log
   lower
   lpad
   ltrim
   max
   md5
   mean
   min
   minute
   mod
   month
   months-between
   nan?
   negate
   next-day
   not
   ntile
   null-count
   null-rate
   null?
   percent-rank
   pi
   pow
   quarter
   rand
   randn
   rank
   regexp-extract
   regexp-replace
   reverse
   rlike
   round
   row-number
   rpad
   rtrim
   second
   sha1
   sha2
   shuffle
   sin
   sinh
   size
   skewness
   slice
   sort-array
   spark-partition-id
   split
   sqr
   sqrt
   starts-with
   stddev
   stddev-pop
   stddev-samp
   substring
   sum
   sum-distinct
   tan
   tanh
   to-date
   trim
   unix-timestamp
   upper
   var-pop
   var-samp
   variance
   week-of-year
   when
   year
   ||])

(defn explain [dataframe] (.explain dataframe))

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

(defn print-schema [dataframe]
  (-> dataframe .schema .treeString println))

(defn random-split
  ([dataframe weights] (.randomSplit dataframe (double-array weights)))
  ([dataframe weights seed] (.randomSplit dataframe (double-array weights) seed)))

(defn empty? [dataframe] (.isEmpty dataframe))

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
(defn sort-within-partitions [dataframe & exprs]
  (.sortWithinPartitions dataframe (->col-array exprs)))
(defn partitions [dataframe] (seq (.. dataframe rdd partitions)))

(defn distinct [dataframe]
  (.distinct dataframe))

(defn limit [dataframe n-rows]
  (.limit dataframe n-rows))

(defn select [dataframe & exprs]
  (.select dataframe (->col-array exprs)))

(defn order-by [dataframe & exprs]
  (.orderBy dataframe (->col-array exprs)))

(defn dtypes [dataframe]
  (let [dtypes-as-tuples (-> dataframe .dtypes seq)]
    (->> dtypes-as-tuples
         (clojure.core/map interop/scala-tuple->vec)
         (into {})
         keywordize-keys)))

(defn columns [dataframe]
  (-> dataframe .columns seq))
(def column-names columns)

(defn rename-columns [dataframe rename-map]
  (reduce
    (fn [acc-df [old-name new-name]]
      (.withColumnRenamed acc-df old-name new-name))
    dataframe
    rename-map))

(defn with-column [dataframe col-name expr]
  (.withColumn dataframe col-name (->column expr)))

(defn drop [dataframe & col-names]
  (.drop dataframe (into-array java.lang.String col-names)))
(defn drop-duplicates [dataframe & col-names]
  (if (clojure.core/empty? col-names)
    (.dropDuplicates dataframe)
    (.dropDuplicates dataframe (into-array java.lang.String col-names))))

(defn except [dataframe other] (.except dataframe other))
(defn intersect [dataframe other] (.intersect dataframe other))

(defn filter [dataframe expr]
  (.filter dataframe expr))
(def where filter)

(defn describe [dataframe & column-names]
  (.describe dataframe (into-array java.lang.String column-names)))
(defn summary [dataframe & stat-names]
  (.summary dataframe (into-array java.lang.String stat-names)))

(defn group-by [dataframe & exprs]
  (.groupBy dataframe (->col-array exprs)))

(defn pivot
  ([grouped expr] (.pivot grouped (->column expr)))
  ([grouped expr values] (.pivot grouped (->column expr) (interop/->scala-seq values))))

(defn agg [dataframe & exprs]
  (let [[head & tail] (clojure.core/map ->column (clojure.core/flatten exprs))]
    (.agg dataframe head (into-array Column tail))))

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

(defn cache [dataframe] (.cache dataframe))
(defn persist [dataframe] (.persist dataframe))

(defmulti count class)
(defmethod count org.apache.spark.sql.Column [x] (functions/count x))
(defmethod count java.lang.String [x] (functions/count x))
(defmethod count org.apache.spark.sql.Dataset [x] (.count x))

(defn isin [expr coll] (.isin (->column expr) (interop/->scala-seq coll)))

(defmulti coalesce (fn [head & _] (class head)))
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions))
(defmethod coalesce :default [& exprs]
  (functions/coalesce (->col-array exprs)))

(defn new-window []
  (Window/partitionBy (->col-array [])))
(defn set-partition-by [window-spec & exprs]
  (.partitionBy window-spec (->col-array exprs)))
(defn set-order-by [window-spec & exprs]
  (.orderBy window-spec (->col-array exprs)))
(defn set-range-between [window-spec start end]
  (.rangeBetween window-spec start end))
(defn set-rows-between [window-spec start end]
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

(defn agg-all [dataframe agg-fn]
  (let [agg-cols (clojure.core/map agg-fn (column-names dataframe))]
    (apply agg dataframe agg-cols)))

(defn sample
  ([dataframe fraction] (.sample dataframe fraction))
  ([dataframe fraction with-replacement]
   (.sample dataframe with-replacement fraction)))

(defn union [& dfs] (reduce #(.union %1 %2) dfs))
(defn union-by-name [& dfs] (reduce #(.unionByName %1 %2) dfs))

(defn collect [dataframe]
  (let [spark-rows (.collect dataframe)
        col-names  (column-names dataframe)]
    (for [row spark-rows]
      (->> row
           interop/spark-row->vec
           (clojure.core/map interop/->clojure)
           (clojure.core/map vector col-names)
           (into {})
           keywordize-keys))))

(defn collect-vals [dataframe]
  (clojure.core/map vals (collect dataframe)))
(defn collect-col [dataframe col-name]
  (clojure.core/map (keyword col-name) (-> dataframe (select col-name) collect)))

(defn take [dataframe n-rows] (-> dataframe (limit n-rows) collect))
(defn take-vals [dataframe n-rows] (-> dataframe (limit n-rows) collect-vals))

(defmulti first class)
(defmethod first Dataset [dataframe] (-> dataframe (take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))
(defn first-vals [dataframe] (-> dataframe (take-vals 1) clojure.core/first))

(defn join
  ([left right join-cols] (join left right join-cols "inner"))
  ([left right join-cols join-type]
   (let [join-cols (if (string? join-cols) [join-cols] join-cols)]
     (.join left right (interop/->scala-seq join-cols) join-type))))

(defn cross-join [left right] (.crossJoin left right))

(defn create-spark-session [{:keys [app-name master configs log-level]
                             :or   {app-name  "Geni App"
                                    master    "local[*]"
                                    configs   {}
                                    log-level "ERROR"}}]
  (let [unconfigured (.. (SparkSession/builder)
                         (appName app-name)
                         (master master))
        configured   (reduce
                       (fn [s [k v]] (.config s k v))
                       unconfigured
                       configs)
        session      (.getOrCreate configured)
        context      (.sparkContext session)]
    (.setLogLevel context log-level)
    session))

(import-vars
  [zero-one.geni.dataset
   ->row
   infer-schema
   infer-struct-field
   java-type->spark-type
   map->dataset
   records->dataset
   table->dataset])

(import-vars
  [zero-one.geni.data-sources
   read-csv!
   read-json!
   read-libsvm!
   read-parquet!
   read-text!
   write-csv!
   write-json!
   write-libsvm!
   write-parquet!
   write-text!])

(comment

  (require '[zero-one.geni.test-resources :refer [spark melbourne-df]])
  (-> melbourne-df count)
  (-> melbourne-df print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (complement :slow))

  (require '[clojure.reflect :as r])
  (->> (r/reflect (.stat melbourne-df))
       :members
       (clojure.core/filter #(= (:name %) 'approxQuantile))
       (mapv :parameter-types)
       pprint)

  0)
