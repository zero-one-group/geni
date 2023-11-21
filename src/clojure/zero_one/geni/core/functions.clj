(ns zero-one.geni.core.functions
  (:refer-clojure :exclude [abs
                            concat
                            flatten
                            hash
                            map
                            not
                            rand
                            reverse
                            second
                            sequence
                            struct
                            when])
  (:require
   [potemkin :refer [import-fn]]
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [->string-map]])
  (:import
   (org.apache.spark.sql Column functions)))

;;;; Agg Functions
(defn approx-count-distinct
  ([expr] (functions/approx_count_distinct (->column expr)))
  ([expr rsd] (functions/approx_count_distinct (->column expr) rsd)))
(defn count-distinct [& exprs]
  (let [[head & tail] (->col-array exprs)]
    (functions/countDistinct head (into-array Column tail))))
(defn grouping [expr] (functions/grouping (->column expr)))
(defn grouping-id [& exprs]
  (functions/grouping_id (interop/->scala-seq (->col-array exprs))))

;;;; Collection Functions
(defn aggregate
  ([expr init merge-fn] (aggregate expr init merge-fn identity))
  ([expr init merge-fn finish-fn]
   (functions/aggregate (->column expr)
                        (->column init)
                        (interop/->scala-function2 merge-fn)
                        (interop/->scala-function1 finish-fn))))
(defn array-contains [expr value]
  (functions/array_contains (->column expr) value))
(defn array-distinct [expr]
  (functions/array_distinct (->column expr)))
(defn array-except [left right]
  (functions/array_except (->column left) (->column right)))
(defn array-intersect [left right]
  (functions/array_intersect (->column left) (->column right)))
(defn array-join
  ([expr delimiter] (functions/array_join (->column expr) delimiter))
  ([expr delimiter null-replacement]
   (functions/array_join (->column expr) delimiter null-replacement)))
(defn array-max [expr]
  (functions/array_max (->column expr)))
(defn array-min [expr]
  (functions/array_min (->column expr)))
(defn array-position [expr value]
  (functions/array_position (->column expr) value))
(defn array-remove [expr element]
  (functions/array_remove (->column expr) element))
(defn array-repeat [left right]
  (if (nat-int? right)
    (functions/array_repeat (->column left) right)
    (functions/array_repeat (->column left) (->column right))))
(defn array-sort [expr]
  (functions/array_sort (->column expr)))
(defn array-union [left right]
  (functions/array_union (->column left) (->column right)))
(defn arrays-overlap [left right]
  (functions/arrays_overlap (->column left) (->column right)))
(defn arrays-zip [& exprs]
  (functions/arrays_zip (->col-array exprs)))
(defn collect-list [expr] (functions/collect_list (->column expr)))
(defn collect-set [expr] (functions/collect_set (->column expr)))
(defn concat [& exprs] (functions/concat (->col-array exprs)))
(defn exists [expr predicate]
  (functions/exists (->column expr) (interop/->scala-function1 predicate)))
(defn explode [expr] (functions/explode (->column expr)))
(defn element-at [expr value]
  (functions/element_at (->column expr) (int value)))
(defn flatten [expr] (functions/flatten (->column expr)))
(defn forall [expr predicate]
  (functions/forall (->column expr) (interop/->scala-function1 predicate)))
(defn from-csv
  ([expr schema] (functions/from_csv (->column expr) (->column schema) {}))
  ([expr schema options]
   (functions/from_csv (->column expr) (->column schema) (->string-map options))))
(defn from-json
  ([expr schema] (functions/from_json (->column expr) (->column schema) {}))
  ([expr schema options]
   (functions/from_json (->column expr) (->column schema) (->string-map options))))
(defn map-concat [& exprs] (functions/map_concat (->col-array exprs)))
(defn map-entries [expr] (functions/map_entries (->column expr)))
(defn map-filter [expr predicate]
  (functions/map_filter (->column expr) (interop/->scala-function2 predicate)))
(defn map-from-entries [expr] (functions/map_from_entries (->column expr)))
(defn map-keys [expr] (functions/map_keys (->column expr)))
(defn map-values [expr] (functions/map_values (->column expr)))
(defn map-zip-with [left right merge-fn]
  (functions/map_zip_with (->column left) (->column right) (interop/->scala-function3 merge-fn)))
(defn posexplode [expr] (functions/posexplode (->column expr)))
(defn reverse [expr]
  (functions/reverse (->column expr)))
(defn schema-of-csv
  ([expr] (functions/schema_of_csv (->column expr)))
  ([expr options] (functions/schema_of_csv (->column expr) (->string-map options))))
(defn schema-of-json
  ([expr] (functions/schema_of_json (->column expr)))
  ([expr options] (functions/schema_of_json (->column expr) (->string-map options))))
(defn sequence [start stop step]
  (functions/sequence (->column start) (->column stop) (->column step)))
(defn size [expr]
  (functions/size (->column expr)))
(defn slice [expr start length]
  (functions/slice (->column expr) start length))
(defn sort-array
  ([expr] (functions/sort_array (->column expr)))
  ([expr asc] (functions/sort_array (->column expr) asc)))
(defn to-csv
  ([expr] (functions/to_csv (->column expr) {}))
  ([expr options]
   (functions/to_csv (->column expr) (->string-map options))))
(defn transform [expr xform-fn]
  (functions/transform (->column expr) (interop/->scala-function1 xform-fn)))
(defn transform-keys [expr key-fn]
  (functions/transform_keys (->column expr) (interop/->scala-function2 key-fn)))
(defn transform-values [expr key-fn]
  (functions/transform_values (->column expr) (interop/->scala-function2 key-fn)))
(defn zip-with [left right merge-fn]
  (functions/zip_with (->column left)
                      (->column right)
                      (interop/->scala-function2 merge-fn)))

;;;; Date and Time Functions
(defn add-months [expr months]
  (functions/add_months (->column expr) months))
(defn current-date [] (functions/current_date))
(defn current-timestamp [] (functions/current_timestamp))
(defn date-add [expr days]
  (functions/date_add (->column expr) days))
(defn date-format [expr date-fmt]
  (functions/date_format (->column expr) date-fmt))
(defn date-sub [expr days]
  (functions/date_sub (->column expr) days))
(defn date-trunc [fmt expr]
  (functions/date_trunc fmt (->column expr)))
(defn datediff [l-expr r-expr]
  (functions/datediff (->column l-expr) (->column r-expr)))
(defn dayofmonth [expr] (functions/dayofmonth (->column expr)))
(defn dayofweek [expr] (functions/dayofweek (->column expr)))
(defn dayofyear [expr] (functions/dayofyear (->column expr)))
(defn from-unixtime
  ([expr] (functions/from_unixtime (->column expr)))
  ([expr fmt] (functions/from_unixtime (->column expr) fmt)))
(defn hour [expr] (functions/hour (->column expr)))
(defn last-day [expr] (functions/last_day (->column expr)))
(defn minute [expr] (functions/minute (->column expr)))
(defn month [expr] (functions/month (->column expr)))
(defn months-between [l-expr r-expr]
  (functions/months_between (->column l-expr) (->column r-expr)))
(defn next-day [expr day-of-week]
  (functions/next_day (->column expr) day-of-week))
(defn quarter [expr] (functions/quarter (->column expr)))
(defn second [expr] (functions/second (->column expr)))
(defn to-date
  ([expr] (functions/to_date (->column expr)))
  ([expr date-format] (functions/to_date (->column expr) date-format)))
(defn to-timestamp
  ([expr] (functions/to_timestamp (->column expr)))
  ([expr date-format] (functions/to_timestamp (->column expr) date-format)))
(defn to-utc-timestamp [expr]
  (functions/to_timestamp (->column expr)))
(defn unix-timestamp
  ([] (functions/unix_timestamp))
  ([expr] (functions/unix_timestamp (->column expr)))
  ([expr pattern] (functions/unix_timestamp (->column expr) pattern)))
(defn window
  ([time-expr duration] (functions/window (->column time-expr) duration))
  ([time-expr duration slide] (functions/window (->column time-expr) duration slide))
  ([time-expr duration slide start] (functions/window (->column time-expr) duration slide start)))
(defn weekofyear [expr] (functions/weekofyear (->column expr)))
(defn year [expr] (functions/year (->column expr)))

;;;; Maths Functions
(def pi
  "The double value that is closer than any other to pi, the ratio of the circumference of a circle to its diameter."
  (functions/lit Math/PI))
(defn abs [expr] (functions/abs (->column expr)))
(defn acos [expr] (functions/acos (->column expr)))
(defn asin [expr] (functions/asin (->column expr)))
(defn atan [expr] (functions/atan (->column expr)))
(defn atan-2 [expr-x expr-y] (functions/atan2 (->column expr-x) (->column expr-y)))
(defn bin [expr] (functions/bin (->column expr)))
(defn bround [expr] (functions/bround (->column expr)))
(defn cbrt [expr] (functions/cbrt (->column expr)))
(defn ceil [expr] (functions/ceil (->column expr)))
(defn conv [expr from-base to-base] (functions/conv (->column expr) from-base to-base))
(defn cos [expr] (functions/cos (->column expr)))
(defn cosh [expr] (functions/cosh (->column expr)))
(defn degrees [expr] (functions/degrees (->column expr)))
(defn exp [expr] (functions/exp (->column expr)))
(defn expm-1 [expr] (functions/expm1 (->column expr)))
(defn factorial [expr] (functions/factorial (->column expr)))
(defn floor [expr] (functions/floor (->column expr)))
(defn hex [expr] (functions/hex (->column expr)))
(defn hypot [left-expr right-expr] (functions/hypot (->column left-expr) (->column right-expr)))
(defn log [expr] (functions/log (->column expr)))
(defn log-10 [expr] (functions/log10 (->column expr)))
(defn log-1p [expr] (functions/log1p (->column expr)))
(defn log-2 [expr] (functions/log2 (->column expr)))
(defn pmod [left-expr right-expr] (functions/pmod (->column left-expr) (->column right-expr)))
(defn pow [base exponent] (functions/pow (->column base) (->column exponent)))
(defn radians [expr] (functions/radians (->column expr)))
(defn rint [expr] (functions/rint (->column expr)))
(defn round [expr] (functions/round (->column expr)))
(defn shift-left [expr num-bits] (functions/shiftLeft (->column expr) num-bits))
(defn shift-right [expr num-bits] (functions/shiftRight (->column expr) num-bits))
(defn shift-right-unsigned [expr num-bits] (functions/shiftRightUnsigned (->column expr) num-bits))
(defn signum [expr] (functions/signum (->column expr)))
(defn sin [expr] (functions/sin (->column expr)))
(defn sinh [expr] (functions/sinh (->column expr)))
(defn sqr
  "Returns the value of the first argument raised to the power of two."
  [expr]
  (.multiply (->column expr) (->column expr)))
(defn sqrt [expr] (functions/sqrt (->column expr)))
(defn tan [expr] (functions/tan (->column expr)))
(defn tanh [expr] (functions/tanh (->column expr)))
(defn unhex [expr] (functions/unhex (->column expr)))

;;;; Misc Functions
(defn crc-32 [expr] (functions/crc32 (->column expr)))
(defn hash [& exprs] (functions/hash (->col-array exprs)))
(defn md-5 [expr] (functions/md5 (->column expr)))
(defn sha-1 [expr] (functions/sha1 (->column expr)))
(defn sha-2 [expr n-bits] (functions/sha2 (->column expr) n-bits))
(defn xxhash-64 [& exprs] (functions/xxhash64 (->col-array exprs)))

;;;; Non-Agg Functions
(defn array [& exprs]
  (functions/array (->col-array exprs)))
(defn bitwise-not [expr] (functions/bitwiseNOT (->column expr)))
(defn broadcast [dataframe] (functions/broadcast dataframe))
(defn expr [s] (functions/expr s))
(defn greatest [& exprs] (functions/greatest (->col-array exprs)))
(defn input-file-name [] (functions/input_file_name))
(defn least [& exprs] (functions/least (->col-array exprs)))
(defn map [& exprs] (functions/map (->col-array exprs)))
(defn map-from-arrays [key-expr val-expr]
  (functions/map_from_arrays (->column key-expr) (->column val-expr)))
(defn monotonically-increasing-id [] (functions/monotonically_increasing_id))
(defn nanvl [left-expr right-expr] (functions/nanvl (->column left-expr) (->column right-expr)))
(defn negate [expr] (functions/negate (->column expr)))
(defn not [expr] (functions/not (->column expr)))
(defn randn
  ([] (functions/randn))
  ([seed] (functions/randn seed)))
(defn rand
  ([] (functions/rand))
  ([seed] (functions/rand seed)))
(defn spark-partition-id [] (functions/spark-partition-id))
(defn struct [& exprs] (functions/struct (->col-array exprs)))
(defn when
  ([condition if-expr]
   (functions/when (->column condition) (->column if-expr)))
  ([condition if-expr else-expr]
   (-> (when condition if-expr) (.otherwise (->column else-expr)))))

;;;; Partition Transform Functions
;(defn bucket [num-buckets expr] (functions/bucket num-buckets (->column expr)))
;(defn days [expr] (functions/days (->column expr)))
;(defn hours [expr] (functions/hours (->column expr)))
;(defn months [expr] (functions/months (->column expr)))
;(defn years [expr] (functions/years (->column expr)))

;;;; String Functions
(defn ascii [expr] (functions/ascii (->column expr)))
(defn base-64 [expr] (functions/base64 (->column expr)))
(defn concat-ws [sep & exprs] (functions/concat_ws sep (->col-array exprs)))
(defn decode [expr charset] (functions/decode (->column expr) charset))
(defn encode [expr charset] (functions/encode (->column expr) charset))
(defn format-number [expr decimal-places]
  (functions/format_number (->column expr) decimal-places))
(defn format-string [fmt & exprs]
  (functions/format_string fmt (->col-array exprs)))
(defn initcap [expr] (functions/initcap (->column expr)))
(defn instr [expr substr] (functions/instr (->column expr) substr))
(defn length [expr] (functions/length (->column expr)))
(defn levenshtein [left-expr right-expr]
  (functions/levenshtein (->column left-expr) (->column right-expr)))
(defn locate [substr expr] (functions/locate substr (->column expr)))
(defn lower [expr] (functions/lower (->column expr)))
(defn lpad [expr length pad] (functions/lpad (->column expr) length pad))
(defn ltrim [expr] (functions/ltrim (->column expr)))
(defn overlay
  ([src rep pos] (functions/overlay (->column src) (->column rep) (->column pos)))
  ([src rep pos len] (functions/overlay (->column src) (->column rep) (->column pos) (->column len))))
(defn regexp-extract [expr regex idx]
  (functions/regexp_extract (->column expr) regex idx))
(defn regexp-replace [expr pattern-expr replacement-expr]
  (functions/regexp_replace
   (->column expr)
   (->column pattern-expr)
   (->column replacement-expr)))
(defn rpad [expr length pad] (functions/rpad (->column expr) length pad))
(defn rtrim [expr] (functions/rtrim (->column expr)))
(defn soundex [expr] (functions/soundex (->column expr)))
(defn split [expr pattern] (functions/split (->column expr) pattern))
(defn substring [expr pos len] (functions/substring (->column expr) pos len))
(defn substring-index [expr delim cnt]
  (functions/substring-index (->column expr) delim cnt))
(defn translate [expr match replacement]
  (functions/translate (->column expr) match replacement))
(defn trim [expr trim-string] (functions/trim (->column expr) trim-string))
(defn unbase-64 [expr] (functions/unbase64 (->column expr)))
(defn upper [expr] (functions/upper (->column expr)))

;;;; Window Functions
(defn cume-dist [] (functions/cume_dist))
(defn dense-rank [] (functions/dense_rank))
(defn lag
  ([expr offset] (functions/lag (->column expr) offset))
  ([expr offset default] (functions/lag (->column expr) offset default)))
(defn lead
  ([expr offset] (functions/lead (->column expr) offset))
  ([expr offset default] (functions/lead (->column expr) offset default)))
(defn ntile [n] (functions/ntile n))
(defn percent-rank [] (functions/percent_rank))
(defn rank [] (functions/rank))
(defn row-number [] (functions/row_number))

;;;; Stats Functions
(defn covar-samp [l-expr r-expr]
  (functions/covar_samp (->column l-expr) (->column r-expr)))
(defn covar-pop [l-expr r-expr] (functions/covar_pop (->column l-expr) (->column r-expr)))
(defn kurtosis [expr] (functions/kurtosis (->column expr)))
(defn skewness [expr] (functions/skewness (->column expr)))
(defn stddev [expr] (functions/stddev (->column expr)))
(defn stddev-pop [expr] (functions/stddev_pop (->column expr)))
(defn sum-distinct [expr] (functions/sumDistinct (->column expr)))
(defn var-pop [expr] (functions/var_pop (->column expr)))
(defn variance [expr] (functions/variance (->column expr)))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.core.functions
 [(-> docs/spark-docs :methods :core :functions)])

;; Aliases
(import-fn atan-2 atan2)
(import-fn base-64 base64)
(import-fn cbrt cube-root)
(import-fn covar-samp covar)
(import-fn crc-32 crc32)
(import-fn datediff date-diff)
(import-fn dayofmonth day-of-month)
(import-fn dayofweek day-of-week)
(import-fn dayofyear day-of-year)
(import-fn explode explode-outer)
(import-fn expm-1 expm1)
(import-fn log-10 log10)
(import-fn log-1p log1p)
(import-fn log-2 log2)
(import-fn md-5 md5)
(import-fn not !)
(import-fn posexplode posexplode-outer)
(import-fn pow **)
(import-fn sha-1 sha1)
(import-fn sha-2 sha2)
(import-fn signum sign)
(import-fn stddev std)
(import-fn stddev stddev-samp)
(import-fn to-date ->date-col)
(import-fn to-timestamp ->timestamp-col)
(import-fn to-utc-timestamp ->utc-timestamp)
(import-fn unbase-64 unbase64)
(import-fn variance var-samp)
(import-fn weekofyear week-of-year)
(import-fn window time-window)
(import-fn xxhash-64 xxhash64)

