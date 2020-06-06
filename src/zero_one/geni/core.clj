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
    [potemkin :refer [import-vars]]
    [zero-one.geni.column]
    [zero-one.geni.dataset]
    [zero-one.geni.data-sources]
    [zero-one.geni.sql]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (org.apache.spark.sql Dataset functions)
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
   isin
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

(import-vars
  [zero-one.geni.dataset
   ->row
   agg
   agg-all
   approx-quantile
   cache
   collect
   collect-col
   collect-vals
   column-names
   columns
   cross-join
   describe
   distinct
   drop
   drop-duplicates
   dtypes
   empty?
   except
   explain
   filter
   first-vals
   group-by
   infer-schema
   infer-struct-field
   intersect
   java-type->spark-type
   join
   limit
   map->dataset
   order-by
   partitions
   persist
   pivot
   print-schema
   random-split
   records->dataset
   rename-columns
   repartition
   repartition-by-range
   sample
   select
   show
   show-vertical
   sort-within-partitions
   summary
   table->dataset
   take
   take-vals
   union
   union-by-name
   where
   with-column])

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

(defmulti count class)
(defmethod count org.apache.spark.sql.Column [x] (functions/count x))
(defmethod count java.lang.String [x] (functions/count x))
(defmethod count org.apache.spark.sql.Dataset [x] (.count x))

(defmulti coalesce (fn [head & _] (class head)))
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions))
(defmethod coalesce :default [& exprs]
  (functions/coalesce (->col-array exprs)))

(defmulti first class)
(defmethod first Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))

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
