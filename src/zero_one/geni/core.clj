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
    [zero-one.geni.interop :as interop]
    [zero-one.geni.sql]
    [zero-one.geni.window])
  (:import
    (org.apache.spark.sql Column
                          Dataset
                          RelationalGroupedDataset
                          SparkSession
                          functions)))

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
   md5
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
  [zero-one.geni.window
   over
   window])

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
(defmethod count :default [x] (functions/count x))
(defmethod count Dataset [x] (.count x))
(defmethod count RelationalGroupedDataset [x] (.count x))

(defmulti mean (fn [x & _] (class x)))
(defmethod mean :default [expr & _] (functions/mean expr))
(defmethod mean RelationalGroupedDataset
  [grouped-ds & col-names] (.mean grouped-ds (interop/->scala-seq col-names)))
(def avg mean)

(defn max [expr] (functions/max expr))
(defn min [expr] (functions/min expr))
(defn sum [expr] (functions/sum expr))

(defmulti coalesce (fn [head & _] (class head)))
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions))
(defmethod coalesce :default [& exprs]
  (functions/coalesce (->col-array exprs)))

(defmulti first class)
(defmethod first Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))

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

  (-> melbourne-df
      (group-by "SellerG")
      (mean "Price" "Rooms")
      show)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (complement :slow))

  (require '[clojure.reflect :as r])
  (->> (r/reflect (.stat melbourne-df))
       :members
       (clojure.core/filter #(= (:name %) 'approxQuantile))
       (mapv :parameter-types)
       pprint)

  0)
