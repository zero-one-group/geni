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
                            remove
                            replace
                            reverse
                            second
                            shuffle
                            sort
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
    (org.apache.spark.sql Dataset
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
   ->timestamp-col
   /
   <
   <=
   ===
   >
   >=
   abs
   acos
   add-months
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
   asc
   asc-nulls-first
   asc-nulls-last
   ascii
   asin
   atan
   atan2
   between
   bin
   broadcast
   cast
   cbrt
   ceil
   collect-list
   collect-set
   concat
   concat-ws
   contains
   conv
   corr
   cos
   cosh
   count-distinct
   covar
   covar-pop
   covar-samp
   cube-root
   cume-dist
   current-date
   current-timestamp
   date-add
   date-diff
   date-format
   date-sub
   date-trunc
   datediff
   day-of-month
   day-of-week
   day-of-year
   degrees
   dense-rank
   desc
   desc-nulls-first
   desc-nulls-last
   element-at
   ends-with
   exp
   explode
   expr
   factorial
   flatten
   floor
   format-number
   format-string
   greatest
   hash
   hour
   isin
   kurtosis
   lag
   last
   last-day
   lead
   least
   length
   levenshtein
   like
   lit
   locate
   log
   log1p
   log2
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
   pmod
   posexplode
   posexplode-outer
   pow
   quarter
   radians
   rand
   randn
   rank
   regexp-extract
   regexp-replace
   reverse
   rint
   rlike
   round
   row-number
   rpad
   rtrim
   second
   sha1
   sha2
   shuffle
   signum
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
   to-timestamp
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
   col-regex
   collect
   collect-col
   collect-vals
   column-names
   columns
   cross-join
   cube
   describe
   distinct
   drop
   drop-duplicates
   drop-na
   dtypes
   empty?
   except
   except-all
   explain
   fill-na
   filter
   first-vals
   group-by
   infer-schema
   infer-struct-field
   intersect
   intersect-all
   is-empty
   is-local
   java-type->spark-type
   join
   join-with
   limit
   local?
   map->dataset
   order-by
   partitions
   persist
   pivot
   print-schema
   random-split
   records->dataset
   remove
   rename-columns
   repartition
   repartition-by-range
   replace
   rollup
   sample
   select
   select-expr
   show
   show-vertical
   sort
   sort-within-partitions
   spark-session
   sql-context
   summary
   table->dataset
   take
   take-vals
   union
   union-by-name
   where
   with-column
   with-column-renamed])

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

(defmulti as (fn [head & _] (class head)))
(defmethod as :default [expr new-name] (.as (->column expr) new-name))
(defmethod as Dataset [dataframe new-name] (.as dataframe new-name))
(def alias as)

(defmulti count class)
(defmethod count :default [expr] (functions/count expr))
(defmethod count Dataset [dataset] (.count dataset))
(defmethod count RelationalGroupedDataset [grouped] (.count grouped))

(defmulti mean (fn [head & _] (class head)))
(defmethod mean :default [expr & _] (functions/mean expr))
(defmethod mean RelationalGroupedDataset
  [grouped & col-names] (.mean grouped (interop/->scala-seq col-names)))
(def avg mean)

(defmulti max (fn [head & _] (class head)))
(defmethod max :default [expr] (functions/max expr))
(defmethod max RelationalGroupedDataset
  [grouped & col-names] (.max grouped (interop/->scala-seq col-names)))

(defmulti min (fn [head & _] (class head)))
(defmethod min :default [expr] (functions/min expr))
(defmethod min RelationalGroupedDataset
  [grouped & col-names] (.min grouped (interop/->scala-seq col-names)))

(defmulti sum (fn [head & _] (class head)))
(defmethod sum :default [expr] (functions/sum expr))
(defmethod sum RelationalGroupedDataset
  [grouped & col-names] (.sum grouped (interop/->scala-seq col-names)))

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

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (complement :slow))

  ;; TODO: add remove (i.e. filter not)
  (require '[clojure.reflect :as r])
  (->> (r/reflect temp)
       :members
       ;(clojure.core/filter #(= (:name %) 'approxQuantile))
       ;(mapv :parameter-types)
       (mapv :name)
       pprint)

  0)
