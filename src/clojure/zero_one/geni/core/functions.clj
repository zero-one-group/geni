(ns zero-one.geni.core.functions
  (:refer-clojure :exclude [abs
                            bit-and
                            bit-or
                            bit-xor
                            char
                            concat
                            flatten
                            get
                            hash
                            map
                            not
                            printf
                            rand
                            reduce
                            repeat
                            reverse
                            second
                            sequence
                            some
                            struct
                            when])
  (:require
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.core.function-table :refer [def-spark-functions]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [->string-map import-fn]])
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
(defn reduce
  "Folds the array column `expr` from `init`: `merge-fn` takes the
  accumulator and an element as columns, and `finish-fn`, when given, turns
  the result into the final value, as `aggregate` does.

  ```clojure
  (g/reduce :scores (g/lit 0) g/+)
  ```"
  ([expr init merge-fn]
   (functions/reduce (->column expr) (->column init) (interop/->scala-function2 merge-fn)))
  ([expr init merge-fn finish-fn]
   (functions/reduce (->column expr)
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
(defn exists
  "With a column and a predicate, returns whether the predicate holds for any
  element of the array column. With a Dataset, returns a column for an EXISTS
  subquery: true when the Dataset has rows, which needs Spark 4.0.

  ```clojure
  (g/exists :scores #(g/> % 90))
  (g/filter orders (g/exists (g/filter refunds (g/=== :order-id (g/outer :id)))))
  ```"
  ([dataframe]
   (spark/require-version! [4 0] "exists over a Dataset")
   (.exists dataframe))
  ([expr predicate]
   (functions/exists (->column expr) (interop/->scala-function1 predicate))))
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
(defn cbrt [expr] (functions/cbrt (->column expr)))
(defn conv [expr from-base to-base] (functions/conv (->column expr) from-base to-base))
(defn cos [expr] (functions/cos (->column expr)))
(defn cosh [expr] (functions/cosh (->column expr)))
(defn degrees [expr] (functions/degrees (->column expr)))
(defn exp [expr] (functions/exp (->column expr)))
(defn expm-1 [expr] (functions/expm1 (->column expr)))
(defn factorial [expr] (functions/factorial (->column expr)))
(defn hex [expr] (functions/hex (->column expr)))
(defn hypot [left-expr right-expr] (functions/hypot (->column left-expr) (->column right-expr)))
(defn log-10 [expr] (functions/log10 (->column expr)))
(defn log-1p [expr] (functions/log1p (->column expr)))
(defn log-2 [expr] (functions/log2 (->column expr)))
(defn pmod [left-expr right-expr] (functions/pmod (->column left-expr) (->column right-expr)))
(defn pow [base exponent] (functions/pow (->column base) (->column exponent)))
(defn radians [expr] (functions/radians (->column expr)))
(defn rint [expr] (functions/rint (->column expr)))
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
(defn lower [expr] (functions/lower (->column expr)))
(defn lpad [expr length pad] (functions/lpad (->column expr) length pad))
(defn ltrim
  ([expr] (functions/ltrim (->column expr)))
  ([expr trim-string] (functions/ltrim (->column expr) trim-string)))
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
(defn rtrim
  ([expr] (functions/rtrim (->column expr)))
  ([expr trim-string] (functions/rtrim (->column expr) trim-string)))
(defn soundex [expr] (functions/soundex (->column expr)))
(defn substring [expr pos len] (functions/substring (->column expr) pos len))
(defn substring-index [expr delim cnt]
  (functions/substring-index (->column expr) delim cnt))
(defn translate [expr match replacement]
  (functions/translate (->column expr) match replacement))
(defn trim
  ([expr] (functions/trim (->column expr)))
  ([expr trim-string] (functions/trim (->column expr) trim-string)))
(defn unbase-64 [expr] (functions/unbase64 (->column expr)))
(defn upper [expr] (functions/upper (->column expr)))

;;;; Window Functions
(defn cume-dist [] (functions/cume_dist))
(defn dense-rank [] (functions/dense_rank))
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

;;;; Spark's functions, from a table
;; A row per function: its name, its argument lists, and then :since for the
;; Spark version that added it, after 3.5, and :spark for Spark's name, when
;; it isn't the Geni name in snake case. See zero-one.geni.core.function-table,
;; and zero-one.geni.function-docs in dev/ for the docstrings.
(def-spark-functions
  [acosh [e]]
  [aes-decrypt [input key] [input key mode] [input key mode padding] [input key mode padding aad]]
  [aes-encrypt [input key] [input key mode] [input key mode padding] [input key mode padding iv] [input key mode padding iv aad]]
  [any [e]]
  [any-value [e] [e ignore-nulls]]
  [approx-percentile [e percentage accuracy]]
  [array-agg [e]]
  [array-append [column element]]
  [array-compact [column]]
  [array-insert [arr pos value]]
  [array-prepend [column element]]
  [array-size [e]]
  [asinh [e]]
  [assert-true [c] [c e]]
  [atanh [e]]
  [bit-and [e]]
  [bit-count [e]]
  [bit-get [e pos]]
  [bit-length [e]]
  [bit-or [e]]
  [bit-xor [e]]
  [bitmap-and-agg [col] :since "4.1"]
  [bitmap-bit-position [col]]
  [bitmap-bucket-number [col]]
  [bitmap-construct-agg [col]]
  [bitmap-count [col]]
  [bitmap-or-agg [col]]
  [bool-and [e]]
  [bool-or [e]]
  [bround [e] [e scale]]
  [btrim [str] [str trim]]
  [bucket [num-buckets e]]
  [call-function [func-name & cols]]
  [call-udf [udf-name & cols]]
  [cardinality [e]]
  [ceil [e] [e scale]]
  [ceiling [e] [e scale]]
  [char [n]]
  [char-length [str]]
  [character-length [str]]
  [chr [n]]
  [collate [e collation] :since "4.0"]
  [collation [e] :since "4.0"]
  [convert-timezone [target-tz source-ts] [source-tz target-tz source-ts]]
  [cot [e]]
  [count-if [e]]
  [csc [e]]
  [curdate []]
  [current-catalog []]
  [current-database []]
  [current-path [] :since "4.2"]
  [current-schema []]
  [current-time [] [precision] :since "4.1"]
  [current-timezone []]
  [current-user []]
  [date-from-unix-date [days]]
  [date-part [field source]]
  [dateadd [start days]]
  [datepart [field source]]
  [day [e]]
  [dayname [time-exp] :since "4.0"]
  [days [e]]
  [e []]
  [elt [& inputs]]
  [endswith [str suffix]]
  [equal-null [col1 col2]]
  [every [e]]
  [explode-outer [e]]
  [extract [field source]]
  [find-in-set [str str-array]]
  [first-value [e] [e ignore-nulls]]
  [floor [e] [e scale]]
  [from-utc-timestamp [ts tz]]
  [from-xml [e schema] :since "4.0"]
  [get [column index]]
  [get-json-object [e path]]
  [getbit [e pos]]
  [histogram-numeric [e n-bins]]
  [hll-sketch-agg [e] [e lg-config-k]]
  [hll-sketch-estimate [c]]
  [hll-union [c1 c2] [c1 c2 allow-different-lg-config-k]]
  [hll-union-agg [e] [e allow-different-lg-config-k]]
  [hours [e]]
  [ifnull [col1 col2]]
  [inline [e]]
  [inline-outer [e]]
  [input-file-block-length []]
  [input-file-block-start []]
  [is-valid-utf8 [str] :since "4.0"]
  [is-valid-variant [v] :since "4.2"]
  [is-variant-null [v] :since "4.0"]
  [isnan [e]]
  [isnotnull [col]]
  [isnull [e]]
  [java-method [& cols]]
  [json-array-length [e]]
  [json-object-keys [e]]
  [json-tuple [json & fields]]
  [kll-merge-agg-bigint [e] [e k] :since "4.1.2"]
  [kll-merge-agg-double [e] [e k] :since "4.1.2"]
  [kll-merge-agg-float [e] [e k] :since "4.1.2"]
  [kll-sketch-agg-bigint [e] [e k] :since "4.1"]
  [kll-sketch-agg-double [e] [e k] :since "4.1"]
  [kll-sketch-agg-float [e] [e k] :since "4.1"]
  [kll-sketch-get-n-bigint [e] :since "4.1"]
  [kll-sketch-get-n-double [e] :since "4.1"]
  [kll-sketch-get-n-float [e] :since "4.1"]
  [kll-sketch-get-quantile-bigint [sketch rank] :since "4.1"]
  [kll-sketch-get-quantile-double [sketch rank] :since "4.1"]
  [kll-sketch-get-quantile-float [sketch rank] :since "4.1"]
  [kll-sketch-get-rank-bigint [sketch quantile] :since "4.1"]
  [kll-sketch-get-rank-double [sketch quantile] :since "4.1"]
  [kll-sketch-get-rank-float [sketch quantile] :since "4.1"]
  [kll-sketch-merge-bigint [left right] :since "4.1"]
  [kll-sketch-merge-double [left right] :since "4.1"]
  [kll-sketch-merge-float [left right] :since "4.1"]
  [kll-sketch-to-string-bigint [e] :since "4.1"]
  [kll-sketch-to-string-double [e] :since "4.1"]
  [kll-sketch-to-string-float [e] :since "4.1"]
  [lag [e offset] [e offset default-value] [e offset default-value ignore-nulls]]
  [last-value [e] [e ignore-nulls]]
  [lcase [str]]
  [lead [e offset] [e offset default-value] [e offset default-value ignore-nulls]]
  [left [str len]]
  [len [e]]
  [levenshtein [l r] [l r threshold]]
  [listagg [e] [e delimiter] :since "4.0"]
  [listagg-distinct [e] [e delimiter] :since "4.0"]
  [ln [e]]
  [localtimestamp []]
  [locate [substr str] [substr str pos]]
  [log [e] [base a]]
  [make-date [year month day]]
  [make-dt-interval [] [days] [days hours] [days hours mins] [days hours mins secs]]
  [make-interval [] [years] [years months] [years months weeks] [years months weeks days] [years months weeks days hours] [years months weeks days hours mins] [years months weeks days hours mins secs]]
  [make-time [hour minute second] :since "4.1"]
  [make-timestamp [date time] [date time timezone] [years months days hours mins secs] [years months days hours mins secs timezone]]
  [make-timestamp-ltz [years months days hours mins secs] [years months days hours mins secs timezone]]
  [make-timestamp-ntz [date time] [years months days hours mins secs]]
  [make-valid-utf8 [str] :since "4.0"]
  [make-ym-interval [] [years] [years months]]
  [map-contains-key [column key]]
  [mask [input] [input upper-char] [input upper-char lower-char] [input upper-char lower-char digit-char] [input upper-char lower-char digit-char other-char]]
  [max-by [e ord] [e ord k]]
  [min-by [e ord] [e ord k]]
  [mode [e] [e deterministic]]
  [monthname [time-exp] :since "4.0"]
  [months [e]]
  [months-between [end start] [end start round-off]]
  [named-struct [& cols]]
  [negative [e]]
  [now []]
  [nth-value [e offset] [e offset ignore-nulls]]
  [nullif [col1 col2]]
  [nullifzero [col] :since "4.0"]
  [nvl [col1 col2]]
  [nvl2 [col1 col2 col3]]
  [octet-length [e]]
  [parse-url [url part-to-extract] [url part-to-extract key]]
  [percentile [e percentage] [e percentage frequency]]
  [percentile-approx [e percentage accuracy]]
  [posexplode-outer [e]]
  [position [substr str] [substr str start]]
  [positive [e]]
  [power [l r]]
  [printf [format & arguments]]
  [product [e]]
  [quote [str] :since "4.1"]
  [raise-error [c]]
  [random [] [seed]]
  [randstr [length] [length seed] :since "4.0"]
  [reflect [& cols]]
  [regexp [str regexp]]
  [regexp-count [str regexp]]
  [regexp-extract-all [str regexp] [str regexp idx]]
  [regexp-instr [str regexp] [str regexp idx]]
  [regexp-like [str regexp]]
  [regexp-substr [str regexp]]
  [regr-avgx [y x]]
  [regr-avgy [y x]]
  [regr-count [y x]]
  [regr-intercept [y x]]
  [regr-r2 [y x]]
  [regr-slope [y x]]
  [regr-sxx [y x]]
  [regr-sxy [y x]]
  [regr-syy [y x]]
  [repeat [str n]]
  [replace-substring [src search] [src search replace] :spark "replace"]
  [right [str len]]
  [round [e] [e scale]]
  [schema-of-variant [v] :since "4.0"]
  [schema-of-variant-agg [v] :since "4.0"]
  [schema-of-xml [xml] :since "4.0"]
  [sec [e]]
  [sentences [string] [string language] [string language country]]
  [sequence [start stop] [start stop step]]
  [session-user [] :since "4.0"]
  [session-window [time-column gap-duration]]
  [sha [col]]
  [shiftleft [e num-bits]]
  [shiftright [e num-bits]]
  [shiftrightunsigned [e num-bits]]
  [sign [e]]
  [some [e]]
  [split [str pattern] [str pattern limit]]
  [split-part [str delimiter part-num]]
  [st-asbinary [geo] [geo endianness] :since "4.1"]
  [st-geogfromwkb [wkb] :since "4.1"]
  [st-geomfromwkb [wkb] [wkb srid] :since "4.1"]
  [st-setsrid [geo srid] :since "4.1"]
  [st-srid [geo] :since "4.1"]
  [stack [& cols]]
  [startswith [str prefix]]
  [str-to-map [text] [text pair-delim] [text pair-delim key-value-delim]]
  [string-agg [e] [e delimiter] :since "4.0"]
  [string-agg-distinct [e] [e delimiter] :since "4.0"]
  [substr [str pos] [str pos len]]
  [theta-difference [c1 c2] :since "4.1"]
  [theta-intersection [c1 c2] :since "4.1"]
  [theta-intersection-agg [e] :since "4.1"]
  [theta-sketch-agg [e] [e lg-nom-entries] :since "4.1"]
  [theta-sketch-estimate [c] :since "4.1"]
  [theta-union [c1 c2] [c1 c2 lg-nom-entries] :since "4.1"]
  [theta-union-agg [e] [e lg-nom-entries] :since "4.1"]
  [time-bucket [bucket-size ts] [bucket-size ts origin] :since "4.2"]
  [time-diff [unit start end] :since "4.1"]
  [time-from-micros [e] :since "4.2"]
  [time-from-millis [e] :since "4.2"]
  [time-from-seconds [e] :since "4.2"]
  [time-to-micros [e] :since "4.2"]
  [time-to-millis [e] :since "4.2"]
  [time-to-seconds [e] :since "4.2"]
  [time-trunc [unit time] :since "4.1"]
  [timestamp-add [unit quantity ts] :since "4.0"]
  [timestamp-diff [unit start end] :since "4.0"]
  [timestamp-micros [e]]
  [timestamp-millis [e]]
  [timestamp-seconds [e]]
  [to-binary [e] [e f]]
  [to-char [e format]]
  [to-number [e format]]
  [to-time [str] [str format] :since "4.1"]
  [to-timestamp-ltz [timestamp] [timestamp format]]
  [to-timestamp-ntz [timestamp] [timestamp format]]
  [to-unix-timestamp [time-exp] [time-exp format]]
  [to-utc-timestamp [ts tz]]
  [to-varchar [e format]]
  [to-variant-object [col] :since "4.0"]
  [to-xml [e] :since "4.0"]
  [trunc [date format]]
  [try-add [left right]]
  [try-aes-decrypt [input key] [input key mode] [input key mode padding] [input key mode padding aad]]
  [try-avg [e]]
  [try-divide [left right]]
  [try-element-at [column value]]
  [try-make-interval [years] [years months] [years months weeks] [years months weeks days] [years months weeks days hours] [years months weeks days hours mins] [years months weeks days hours mins secs] :since "4.0"]
  [try-make-timestamp [date time] [date time timezone] [years months days hours mins secs] [years months days hours mins secs timezone] :since "4.0"]
  [try-make-timestamp-ltz [years months days hours mins secs] [years months days hours mins secs timezone] :since "4.0"]
  [try-make-timestamp-ntz [date time] [years months days hours mins secs] :since "4.0"]
  [try-mod [left right] :since "4.0"]
  [try-multiply [left right]]
  [try-parse-json [json] :since "4.0"]
  [try-parse-url [url part-to-extract] [url part-to-extract key] :since "4.0"]
  [try-reflect [& cols] :since "4.0"]
  [try-subtract [left right]]
  [try-sum [e]]
  [try-to-binary [e] [e f]]
  [try-to-date [e] [e fmt] :since "4.0"]
  [try-to-number [e format]]
  [try-to-time [str] [str format] :since "4.1"]
  [try-to-timestamp [s] [s format]]
  [try-url-decode [str] :since "4.0"]
  [try-validate-utf8 [str] :since "4.0"]
  [try-variant-get [v path target-type] :since "4.0"]
  [tuple-difference-double [c1 c2] :since "4.2"]
  [tuple-difference-integer [c1 c2] :since "4.2"]
  [tuple-difference-theta-double [c1 c2] :since "4.2"]
  [tuple-difference-theta-integer [c1 c2] :since "4.2"]
  [tuple-intersection-agg-double [e] [e mode] :since "4.2"]
  [tuple-intersection-agg-integer [e] [e mode] :since "4.2"]
  [tuple-intersection-double [c1 c2] [c1 c2 mode] :since "4.2"]
  [tuple-intersection-integer [c1 c2] [c1 c2 mode] :since "4.2"]
  [tuple-intersection-theta-double [c1 c2] [c1 c2 mode] :since "4.2"]
  [tuple-intersection-theta-integer [c1 c2] [c1 c2 mode] :since "4.2"]
  [tuple-sketch-agg-double [key summary] [key summary lg-nom-entries] [key summary lg-nom-entries mode] :since "4.2"]
  [tuple-sketch-agg-integer [key summary] [key summary lg-nom-entries] [key summary lg-nom-entries mode] :since "4.2"]
  [tuple-sketch-estimate-double [c] :since "4.2"]
  [tuple-sketch-estimate-integer [c] :since "4.2"]
  [tuple-sketch-summary-double [c] [c mode] :since "4.2"]
  [tuple-sketch-summary-integer [c] [c mode] :since "4.2"]
  [tuple-sketch-theta-double [c] :since "4.2"]
  [tuple-sketch-theta-integer [c] :since "4.2"]
  [tuple-union-agg-double [e] [e lg-nom-entries] [e lg-nom-entries mode] :since "4.2"]
  [tuple-union-agg-integer [e] [e lg-nom-entries] [e lg-nom-entries mode] :since "4.2"]
  [tuple-union-double [c1 c2] [c1 c2 lg-nom-entries] [c1 c2 lg-nom-entries mode] :since "4.2"]
  [tuple-union-integer [c1 c2] [c1 c2 lg-nom-entries] [c1 c2 lg-nom-entries mode] :since "4.2"]
  [tuple-union-theta-double [c1 c2] [c1 c2 lg-nom-entries] [c1 c2 lg-nom-entries mode] :since "4.2"]
  [tuple-union-theta-integer [c1 c2] [c1 c2 lg-nom-entries] [c1 c2 lg-nom-entries mode] :since "4.2"]
  [typeof [col]]
  [ucase [str]]
  [uniform [min max] [min max seed] :since "4.0"]
  [unix-date [e]]
  [unix-micros [e]]
  [unix-millis [e]]
  [unix-seconds [e]]
  [unwrap-udt [column]]
  [url-decode [str]]
  [url-encode [str]]
  [user []]
  [uuid [] [seed]]
  [validate-utf8 [str] :since "4.0"]
  [variant-get [v path target-type] :since "4.0"]
  [weekday [e]]
  [width-bucket [v min max num-bucket]]
  [window-time [window-column]]
  [xpath [xml path]]
  [xpath-boolean [xml path]]
  [xpath-double [xml path]]
  [xpath-float [xml path]]
  [xpath-int [xml path]]
  [xpath-long [xml path]]
  [xpath-number [xml path]]
  [xpath-short [xml path]]
  [xpath-string [xml path]]
  [years [e]]
  [zeroifnull [col] :since "4.0"])

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
(import-fn expm-1 expm1)
(import-fn log-10 log10)
(import-fn log-1p log1p)
(import-fn log-2 log2)
(import-fn md-5 md5)
(import-fn not !)
(import-fn pow **)
(import-fn sha-1 sha1)
(import-fn sha-2 sha2)
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

