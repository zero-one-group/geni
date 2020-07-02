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
                            assoc
                            boolean
                            byte
                            cast
                            concat
                            count
                            dec
                            dissoc
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
                            keys
                            last
                            long
                            map
                            max
                            merge
                            merge-with
                            min
                            mod
                            neg?
                            not
                            odd?
                            partition-by
                            pos?
                            rand
                            remove
                            rename-keys
                            reverse
                            second
                            select-keys
                            sequence
                            short
                            shuffle
                            sort
                            str
                            struct
                            take
                            update
                            vals
                            when
                            zero?
                            zipmap])
  (:require
    [clojure.walk]
    [potemkin :refer [import-vars]]
    [zero-one.geni.column]
    [zero-one.geni.data-sources]
    [zero-one.geni.dataset]
    [zero-one.geni.google-sheets]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.polymorphic]
    [zero-one.geni.sql]
    [zero-one.geni.storage]
    [zero-one.geni.window])
  (:import
    (org.apache.spark.sql SparkSession)))

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
   is-in-collection
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
   str
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
   assoc
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
   dissoc
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
   from-csv
   from-json
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
   keys
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
   map
   map-concat
   map-entries
   map-filter
   map-from-arrays
   map-from-entries
   map-keys
   map-values
   map-zip-with
   md5
   merge
   merge-with
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
   rename-keys
   reverse
   rint
   round
   row-number
   rpad
   rtrim
   schema-of-csv
   schema-of-json
   second
   select-keys
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
   to-csv
   to-date
   to-timestamp
   to-utc-timestamp
   transform
   transform-keys
   transform-values
   translate
   trim
   unbase64
   unhex
   unix-timestamp
   update
   upper
   vals
   var-pop
   var-samp
   variance
   week-of-year
   when
   xxhash64
   year
   zip-with
   zipmap])

(import-vars
  [zero-one.geni.dataset
   ->row
   agg
   agg-all
   approx-quantile
   cache
   checkpoint
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
   fill-na
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
   head
   head-vals
   take
   take-vals
   union
   union-by-name
   unpersist
   with-column
   with-column-renamed])

(import-vars
  [zero-one.geni.window
   over
   unbounded-following
   unbounded-preceeding
   window
   windowed])

(import-vars
  [zero-one.geni.data-sources
   read-avro!
   read-csv!
   read-json!
   read-libsvm!
   read-parquet!
   read-text!
   write-avro!
   write-csv!
   write-json!
   write-libsvm!
   write-parquet!
   write-text!])

(import-vars
  [zero-one.geni.polymorphic
   alias
   as
   coalesce
   count
   explain
   filter
   first
   last
   max
   mean
   min
   shuffle
   sum
   to-json
   where])

(import-vars
  [zero-one.geni.google-sheets
   sheet-names!
   sheet-values!
   read-sheets!
   write-sheets!
   create-sheets!
   delete-sheets!])

(import-vars
  [zero-one.geni.storage
   disk-only
   disk-only-2
   memory-and-disk
   memory-and-disk-2
   memory-and-disk-ser
   memory-and-disk-ser-2
   memory-only
   memory-only-2
   memory-only-ser
   memory-only-ser-2
   none
   off-heap])

(def to-string (memfn toString))
(def ->string to-string)

(def to-debug-string (memfn toDebugString))
(def ->debug-string to-debug-string)

(defn create-spark-session [{:keys [app-name master configs log-level checkpoint-dir]
                             :or   {app-name  "Geni App"
                                    master    "local[*]"
                                    configs   {}
                                    log-level "ERROR"}}]
  (let [unconfigured (.. (SparkSession/builder)
                         (appName app-name)
                         (master master))
        configured   (reduce
                       (fn [s [k v]] (.config s (name k) v))
                       unconfigured
                       configs)
        session      (.getOrCreate configured)
        context      (.sparkContext session)]
    (.setLogLevel context log-level)
    (clojure.core/when checkpoint-dir
      (.setCheckpointDir context checkpoint-dir))
    session))

(defn spark-conf [spark-session]
  (->> spark-session
       .sparkContext
       .getConf
       .getAll
       (clojure.core/map interop/scala-tuple->vec)
       (into {})
       clojure.walk/keywordize-keys))

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
