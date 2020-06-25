(ns zero-one.geni.core
  (:refer-clojure :exclude [*
                            +
                            -
                            /
                            <
                            <=
                            =
                            >
                            >=
                            alias
                            boolean
                            byte
                            cast
                            concat
                            count
                            dec
                            distinct
                            double
                            drop
                            empty?
                            even?
                            filter
                            first
                            flatten
                            float
                            group-by
                            hash
                            inc
                            int
                            last
                            long
                            map
                            max
                            min
                            mod
                            neg?
                            not
                            odd?
                            partition-by
                            pos?
                            rand
                            remove
                            reverse
                            second
                            sequence
                            short
                            shuffle
                            sort
                            struct
                            take
                            when
                            zero?])
  (:require
    [potemkin :refer [import-vars]]
    [zero-one.geni.column]
    [zero-one.geni.dataset]
    [zero-one.geni.data-sources]
    [zero-one.geni.google-sheets]
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
   %
   &
   &&
   *
   +
   -
   ->col-array
   ->column
   /
   <
   <=
   <=>
   =
   =!=
   ===
   >
   >=
   asc
   asc-nulls-first
   asc-nulls-last
   between
   bitwise-and
   bitwise-or
   bitwise-xor
   boolean
   byte
   cast
   col
   contains
   dec
   desc
   desc-nulls-first
   desc-nulls-last
   double
   ends-with
   even?
   float
   get-field
   get-item
   hash-code
   inc
   int
   is-nan
   is-not-null
   is-null
   isin
   like
   lit
   long
   mod
   nan?
   neg?
   not-null?
   null-count
   null-rate
   null?
   odd?
   pos?
   rlike
   short
   starts-with
   zero?
   |
   ||])

(import-vars
  [zero-one.geni.sql
   !
   ->date-col
   ->timestamp-col
   ->utc-timestamp
   ;bucket
   ;days
   ;hours
   ;map
   ;map-from-arrays
   ;months
   ;years
   abs
   acos
   add-months
   aggregate
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
   ascii
   asin
   atan
   atan2
   base64
   bin
   bitwise-not
   broadcast
   bround
   cbrt
   ceil
   collect-list
   collect-set
   concat
   concat-ws
   conv
   corr
   cos
   cosh
   count-distinct
   covar
   covar-pop
   covar-samp
   crc32
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
   decode
   degrees
   dense-rank
   element-at
   encode
   exists
   exp
   explode
   expm1
   expr
   factorial
   flatten
   floor
   forall
   format-number
   format-string
   from-unixtime
   greatest
   grouping
   grouping-id
   hash
   hex
   hour
   hypot
   initcap
   input-file-name
   instr
   kurtosis
   lag
   last-day
   lead
   least
   length
   levenshtein
   locate
   log
   log10
   log1p
   log2
   lower
   lpad
   ltrim
   md5
   minute
   monotonically-increasing-id
   month
   months-between
   nanvl
   negate
   next-day
   not
   ntile
   overlay
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
   round
   row-number
   rpad
   rtrim
   second
   sequence
   sha1
   sha2
   shift-left
   shift-right
   shift-right-unsigned
   signum
   sin
   sinh
   size
   skewness
   slice
   sort-array
   soundex
   spark-partition-id
   split
   sqr
   sqrt
   stddev
   stddev-pop
   stddev-samp
   struct
   substring
   substring-index
   sum-distinct
   tan
   tanh
   time-window
   to-date
   to-timestamp
   to-utc-timestamp
   transform
   translate
   trim
   unbase64
   unhex
   unix-timestamp
   upper
   var-pop
   var-samp
   variance
   week-of-year
   when
   xxhash64
   year
   zip-with])

(import-vars
  [zero-one.geni.dataset
   ->row
   agg
   agg-all
   approx-quantile
   cache
   ;checkpoint
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
   input-files
   intersect
   intersect-all
   is-empty
   is-local
   is-streaming
   java-type->spark-type
   join
   join-with
   last-vals
   limit
   local?
   map->dataset
   order-by
   partitions
   persist
   pivot
   print-schema
   random-split
   rdd
   records->dataset
   remove
   rename-columns
   repartition
   repartition-by-range
   replace-na
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
   storage-level
   streaming?
   summary
   table->dataset
   tail
   tail-vals
   take
   take-vals
   union
   union-by-name
   unpersist
   where
   with-column
   with-column-renamed])

(import-vars
  [zero-one.geni.window
   over
   unbounded-following
   unbounded-preceeding
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

(import-vars
  [zero-one.geni.google-sheets
   sheet-names!
   sheet-values!
   read-sheets!
   write-sheets!
   create-sheets!
   delete-sheets!])

(defmulti as (fn [head & _] (class head)))
(defmethod as :default [expr new-name] (.as (->column expr) (name new-name)))
(defmethod as Dataset [dataframe new-name] (.as dataframe (name new-name)))
(def alias as)

(defmulti count class)
(defmethod count :default [expr] (functions/count (->column expr)))
(defmethod count Dataset [dataset] (.count dataset))
(defmethod count RelationalGroupedDataset [grouped] (.count grouped))

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
  (order-by dataframe (functions/randn)))

(defmulti first class)
(defmethod first Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/take 1) clojure.core/first))
(defmethod first :default [expr] (functions/first (->column expr)))

(defmulti last class)
(defmethod last Dataset [dataframe]
  (-> dataframe (zero-one.geni.dataset/tail 1) clojure.core/first))
(defmethod last :default [expr] (functions/last (->column expr)))

(def to-string (memfn toString))
(def ->string to-string)

(def to-json (memfn toJSON))
(def ->json to-json)

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
  (def dataframe melbourne-df)
  (-> dataframe count)
  (-> dataframe print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (complement :slow))

  (require '[clojure.reflect :as r])
  (->> (r/reflect Dataset)
       :members
       ;(clojure.core/filter #(= (:name %) 'approxQuantile))
       ;(mapv :parameter-types)
       (mapv :name)
       clojure.core/sort
       pprint)

  0)
