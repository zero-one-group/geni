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
   [zero-one.geni.core.column :refer [->column]]
   [zero-one.geni.core.function-table :as function-table :refer [def-spark-functions]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [import-fn]])
  (:import
   (org.apache.spark.sql functions)))

(defn aggregate
  "Folds the array column `expr` from `init`: `merge-fn` takes the
  accumulator and an element as columns, and `finish-fn`, `identity` by
  default, turns the result into the final value. `reduce` does the same.

  ```clojure
  (g/aggregate :scores (g/lit 0) g/+)
  ```"
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

(defn forall
  "Whether `predicate`, a function of a column, holds for every element of
  the array column `expr`.

  ```clojure
  (g/forall :scores #(g/> % 50))
  ```"
  [expr predicate]
  (functions/forall (->column expr) (interop/->scala-function1 predicate)))

(defn map-filter
  "The entries of the map column `expr` for which `predicate`, a function of
  the key and the value as columns, holds."
  [expr predicate]
  (functions/map_filter (->column expr) (interop/->scala-function2 predicate)))

(defn map-zip-with
  "A map of the keys of the map columns `left` and `right`, each with what
  `merge-fn` gives from the key and the two values, as columns, one of them
  null where only one map has the key."
  [left right merge-fn]
  (functions/map_zip_with (->column left) (->column right) (interop/->scala-function3 merge-fn)))

(defn transform
  "The array column `expr` with `xform-fn`, a function of a column, applied
  to each element.

  ```clojure
  (g/transform :scores #(g/* % 2))
  ```"
  [expr xform-fn]
  (functions/transform (->column expr) (interop/->scala-function1 xform-fn)))

(defn transform-keys
  "The map column `expr` with each key replaced by what `key-fn`, a function
  of the key and the value as columns, gives."
  [expr key-fn]
  (functions/transform_keys (->column expr) (interop/->scala-function2 key-fn)))

(defn transform-values
  "The map column `expr` with each value replaced by what `value-fn`, a
  function of the key and the value as columns, gives."
  [expr value-fn]
  (functions/transform_values (->column expr) (interop/->scala-function2 value-fn)))

(defn zip-with
  "An array of what `merge-fn` gives from the elements of the array columns
  `left` and `right` at each position, as columns, with nulls for the
  shorter one's missing elements."
  [left right merge-fn]
  (functions/zip_with (->column left)
                      (->column right)
                      (interop/->scala-function2 merge-fn)))

(defn from-csv
  "Parses the column `expr` of CSV lines into structs, by `schema`, a DDL
  string such as `\"a INT, b STRING\"` or a column such as `schema-of-csv`
  gives, with Spark's CSV `options`.

  ```clojure
  (g/from-csv :line \"id INT, name STRING\" {:sep \";\"})
  ```"
  ([expr schema] (from-csv expr schema {}))
  ([expr schema options]
   (function-table/invoke "from_csv" nil 'from-csv
                          [expr (if (string? schema) (functions/lit schema) schema) options])))

(defn when
  "A column of `if-expr` where `condition` holds, and else of `else-expr`, or
  null without it. `g/cond` takes more branches."
  ([condition if-expr]
   (functions/when (->column condition) (->column if-expr)))
  ([condition if-expr else-expr]
   (-> (when condition if-expr) (.otherwise (->column else-expr)))))

(def pi
  "The double value that is closer than any other to pi, the ratio of the circumference of a circle to its diameter."
  (functions/lit Math/PI))

(defn sqr
  "Returns the value of the first argument raised to the power of two."
  [expr]
  (.multiply (->column expr) (->column expr)))

;;;; Spark's functions, from a table
;; A row per function: its name, its argument lists, and then :since for the
;; Spark version that added it, after 3.5, :arity-since for the versions that
;; added some of its arities, :columns-since for the version that takes a
;; column after its first argument, and :spark for Spark's name, when it isn't
;; the Geni name in snake case. See zero-one.geni.core.function-table, and
;; zero-one.geni.function-docs in dev/ for the docstrings.
(def-spark-functions
  [abs [e]]
  [acos [e]]
  [acosh [e]]
  [add-months [start-date num-months]]
  [aes-decrypt [input key] [input key mode] [input key mode padding] [input key mode padding aad]]
  [aes-encrypt [input key] [input key mode] [input key mode padding] [input key mode padding iv] [input key mode padding iv aad]]
  [any [e]]
  [any-value [e] [e ignore-nulls]]
  [approx-count-distinct [e] [e rsd]]
  [approx-percentile [e percentage accuracy]]
  [array [& cols]]
  [array-agg [e]]
  [array-append [column element]]
  [array-compact [column]]
  [array-contains [column value]]
  [array-distinct [e]]
  [array-except [col1 col2]]
  [array-insert [arr pos value]]
  [array-intersect [col1 col2]]
  [array-join [column delimiter] [column delimiter null-replacement]]
  [array-max [e]]
  [array-min [e]]
  [array-position [column value]]
  [array-prepend [column element]]
  [array-remove [column element]]
  [array-repeat [e count]]
  [array-size [e]]
  [array-sort [e]]
  [array-union [col1 col2]]
  [arrays-overlap [a1 a2]]
  [arrays-zip [& e]]
  [ascii [e]]
  [asin [e]]
  [asinh [e]]
  [assert-true [c] [c e]]
  [atan [e]]
  [atan2 [y x]]
  [atanh [e]]
  [base64 [e]]
  [bin [e]]
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
  [bitwise-not [e]]
  [bool-and [e]]
  [bool-or [e]]
  [broadcast [df]]
  [bround [e] [e scale] :columns-since "4.0"]
  [btrim [str] [str trim]]
  [bucket [num-buckets e]]
  [call-function [func-name & cols]]
  [call-udf [udf-name & cols]]
  [cardinality [e]]
  [cbrt [e]]
  [ceil [e] [e scale]]
  [ceiling [e] [e scale]]
  [char [n]]
  [char-length [str]]
  [character-length [str]]
  [chr [n]]
  [collate [e collation] :since "4.0"]
  [collation [e] :since "4.0"]
  [collect-list [e]]
  [collect-set [e]]
  [concat [& exprs]]
  [concat-ws [sep & exprs]]
  [conv [num from-base to-base]]
  [convert-timezone [target-tz source-ts] [source-tz target-tz source-ts]]
  [cos [e]]
  [cosh [e]]
  [cot [e]]
  [count-distinct [expr & exprs]]
  [count-if [e]]
  [covar-pop [column1 column2]]
  [covar-samp [column1 column2]]
  [crc32 [e]]
  [csc [e]]
  [cume-dist []]
  [curdate []]
  [current-catalog []]
  [current-database []]
  [current-date []]
  [current-path [] :since "4.2"]
  [current-schema []]
  [current-time [] [precision] :since "4.1"]
  [current-timestamp []]
  [current-timezone []]
  [current-user []]
  [date-add [start days]]
  [date-format [date-expr format]]
  [date-from-unix-date [days]]
  [date-part [field source]]
  [date-sub [start days]]
  [date-trunc [format timestamp]]
  [dateadd [start days]]
  [datediff [end start]]
  [datepart [field source]]
  [day [e]]
  [dayname [time-exp] :since "4.0"]
  [dayofmonth [e]]
  [dayofweek [e]]
  [dayofyear [e]]
  [days [e]]
  [decode [value charset]]
  [degrees [e]]
  [dense-rank []]
  [e []]
  [element-at [column value]]
  [elt [& inputs]]
  [encode [value charset]]
  [endswith [str suffix]]
  [equal-null [col1 col2]]
  [every [e]]
  [exp [e]]
  [explode [e]]
  [explode-outer [e]]
  [expm1 [e]]
  [expr [expr]]
  [extract [field source]]
  [factorial [e]]
  [find-in-set [str str-array]]
  [first-value [e] [e ignore-nulls]]
  [flatten [e]]
  [floor [e] [e scale]]
  [format-number [x d]]
  [format-string [format & arguments]]
  [from-json [e schema] [e schema options]]
  [from-unixtime [ut] [ut f]]
  [from-utc-timestamp [ts tz]]
  [from-xml [e schema] :since "4.0"]
  [get [column index]]
  [get-json-object [e path]]
  [getbit [e pos]]
  [greatest [& exprs]]
  [grouping [e]]
  [grouping-id [& cols]]
  [hash [& cols]]
  [hex [column]]
  [histogram-numeric [e n-bins]]
  [hll-sketch-agg [e] [e lg-config-k]]
  [hll-sketch-estimate [c]]
  [hll-union [c1 c2] [c1 c2 allow-different-lg-config-k]]
  [hll-union-agg [e] [e allow-different-lg-config-k]]
  [hour [e]]
  [hours [e]]
  [hypot [l r]]
  [ifnull [col1 col2]]
  [initcap [e]]
  [inline [e]]
  [inline-outer [e]]
  [input-file-block-length []]
  [input-file-block-start []]
  [input-file-name []]
  [instr [str substring]]
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
  [kurtosis [e]]
  [lag [e offset] [e offset default-value] [e offset default-value ignore-nulls]]
  [last-day [e]]
  [last-value [e] [e ignore-nulls]]
  [lcase [str]]
  [lead [e offset] [e offset default-value] [e offset default-value ignore-nulls]]
  [least [& exprs]]
  [left [str len]]
  [len [e]]
  [length [e]]
  [levenshtein [l r] [l r threshold]]
  [listagg [e] [e delimiter] :since "4.0"]
  [listagg-distinct [e] [e delimiter] :since "4.0"]
  [ln [e]]
  [localtimestamp []]
  [locate [substr str] [substr str pos]]
  [log [e] [base a]]
  [log10 [e]]
  [log1p [e]]
  [log2 [expr]]
  [lower [e]]
  [lpad [str len pad]]
  [ltrim [e] [e trim]]
  [make-date [year month day]]
  [make-dt-interval [] [days] [days hours] [days hours mins] [days hours mins secs]]
  [make-interval [] [years] [years months] [years months weeks] [years months weeks days] [years months weeks days hours] [years months weeks days hours mins] [years months weeks days hours mins secs]]
  [make-time [hour minute second] :since "4.1"]
  [make-timestamp [date time] [date time timezone] [years months days hours mins secs] [years months days hours mins secs timezone] :arity-since {2 "4.1" 3 "4.1"}]
  [make-timestamp-ltz [years months days hours mins secs] [years months days hours mins secs timezone]]
  [make-timestamp-ntz [date time] [years months days hours mins secs] :arity-since {2 "4.1"}]
  [make-valid-utf8 [str] :since "4.0"]
  [make-ym-interval [] [years] [years months]]
  [map [& cols]]
  [map-concat [& cols]]
  [map-contains-key [column key]]
  [map-entries [e]]
  [map-from-arrays [keys values]]
  [map-from-entries [e]]
  [map-keys [e]]
  [map-values [e]]
  [mask [input] [input upper-char] [input upper-char lower-char] [input upper-char lower-char digit-char] [input upper-char lower-char digit-char other-char]]
  [max-by [e ord] [e ord k] :arity-since {3 "4.2"}]
  [md5 [e]]
  [min-by [e ord] [e ord k] :arity-since {3 "4.2"}]
  [minute [e]]
  [mode [e] [e deterministic] :arity-since {2 "4.0"}]
  [monotonically-increasing-id []]
  [month [e]]
  [monthname [time-exp] :since "4.0"]
  [months [e]]
  [months-between [end start] [end start round-off]]
  [named-struct [& cols]]
  [nanvl [col1 col2]]
  [negate [e]]
  [negative [e]]
  [next-day [date day-of-week]]
  [not [e]]
  [now []]
  [nth-value [e offset] [e offset ignore-nulls]]
  [ntile [n]]
  [nullif [col1 col2]]
  [nullifzero [col] :since "4.0"]
  [nvl [col1 col2]]
  [nvl2 [col1 col2 col3]]
  [octet-length [e]]
  [overlay [src replace pos] [src replace pos len]]
  [parse-url [url part-to-extract] [url part-to-extract key]]
  [percent-rank []]
  [percentile [e percentage] [e percentage frequency]]
  [percentile-approx [e percentage accuracy]]
  [pmod [dividend divisor]]
  [posexplode [e]]
  [posexplode-outer [e]]
  [position [substr str] [substr str start]]
  [positive [e]]
  [pow [l r]]
  [power [l r]]
  [printf [format & arguments]]
  [product [e]]
  [quarter [e]]
  [quote [str] :since "4.1"]
  [radians [e]]
  [raise-error [c]]
  [rand [] [seed]]
  [randn [] [seed]]
  [random [] [seed]]
  [randstr [length] [length seed] :since "4.0"]
  [rank []]
  [reflect [& cols]]
  [regexp [str regexp]]
  [regexp-count [str regexp]]
  [regexp-extract [e exp group-idx]]
  [regexp-extract-all [str regexp] [str regexp idx]]
  [regexp-instr [str regexp] [str regexp idx]]
  [regexp-like [str regexp]]
  [regexp-replace [e pattern replacement]]
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
  [repeat [str n] :columns-since "4.0"]
  [replace-substring [src search] [src search replace] :spark "replace"]
  [reverse [e]]
  [right [str len]]
  [rint [e]]
  [round [e] [e scale] :columns-since "4.0"]
  [row-number []]
  [rpad [str len pad]]
  [rtrim [e] [e trim]]
  [schema-of-csv [csv] [csv options]]
  [schema-of-json [json] [json options]]
  [schema-of-variant [v] :since "4.0"]
  [schema-of-variant-agg [v] :since "4.0"]
  [schema-of-xml [xml] :since "4.0"]
  [sec [e]]
  [second [e]]
  [sentences [string] [string language] [string language country] :arity-since {2 "4.0"}]
  [sequence [start stop] [start stop step]]
  [session-user [] :since "4.0"]
  [session-window [time-column gap-duration]]
  [sha [col]]
  [sha1 [e]]
  [sha2 [e num-bits]]
  [shiftleft [e num-bits]]
  [shiftright [e num-bits]]
  [shiftrightunsigned [e num-bits]]
  [sign [e]]
  [signum [e]]
  [sin [e]]
  [sinh [e]]
  [size [e]]
  [skewness [e]]
  [slice [x start length]]
  [some [e]]
  [sort-array [e] [e asc]]
  [soundex [e]]
  [spark-partition-id []]
  [split [str pattern] [str pattern limit] :columns-since "4.0"]
  [split-part [str delimiter part-num]]
  [sqrt [e]]
  [st-asbinary [geo] [geo endianness] :since "4.1" :arity-since {2 "4.2"}]
  [st-geogfromwkb [wkb] :since "4.1"]
  [st-geomfromwkb [wkb] [wkb srid] :since "4.1" :arity-since {2 "4.2"}]
  [st-setsrid [geo srid] :since "4.1"]
  [st-srid [geo] :since "4.1"]
  [stack [& cols]]
  [startswith [str prefix]]
  [stddev [e]]
  [stddev-pop [e]]
  [str-to-map [text] [text pair-delim] [text pair-delim key-value-delim]]
  [string-agg [e] [e delimiter] :since "4.0"]
  [string-agg-distinct [e] [e delimiter] :since "4.0"]
  [struct [& cols]]
  [substr [str pos] [str pos len]]
  [substring [str pos len]]
  [substring-index [str delim count]]
  [sum-distinct [e]]
  [tan [e]]
  [tanh [e]]
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
  [to-csv [e] [e options]]
  [to-date [e] [e fmt]]
  [to-number [e format]]
  [to-time [str] [str format] :since "4.1"]
  [to-timestamp [s] [s fmt]]
  [to-timestamp-ltz [timestamp] [timestamp format]]
  [to-timestamp-ntz [timestamp] [timestamp format]]
  [to-unix-timestamp [time-exp] [time-exp format]]
  [to-utc-timestamp [ts tz]]
  [to-varchar [e format]]
  [to-variant-object [col] :since "4.0"]
  [to-xml [e] :since "4.0"]
  [translate [src matching-string replace-string]]
  [trim [e] [e trim]]
  [trunc [date format]]
  [try-add [left right]]
  [try-aes-decrypt [input key] [input key mode] [input key mode padding] [input key mode padding aad]]
  [try-avg [e]]
  [try-divide [left right]]
  [try-element-at [column value]]
  [try-make-interval [years] [years months] [years months weeks] [years months weeks days] [years months weeks days hours] [years months weeks days hours mins] [years months weeks days hours mins secs] :since "4.0"]
  [try-make-timestamp [date time] [date time timezone] [years months days hours mins secs] [years months days hours mins secs timezone] :since "4.0" :arity-since {2 "4.1" 3 "4.1"}]
  [try-make-timestamp-ltz [years months days hours mins secs] [years months days hours mins secs timezone] :since "4.0"]
  [try-make-timestamp-ntz [date time] [years months days hours mins secs] :since "4.0" :arity-since {2 "4.1"}]
  [try-mod [left right] :since "4.0"]
  [try-multiply [left right]]
  [try-parse-json [json] :since "4.0"]
  [try-parse-url [url part-to-extract] [url part-to-extract key] :since "4.0"]
  [try-reflect [& cols] :since "4.0"]
  [try-subtract [left right]]
  [try-sum [e]]
  [try-to-binary [e] [e f]]
  [try-to-date [e] [e fmt] :since "4.1"]
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
  [unbase64 [e]]
  [unhex [column]]
  [uniform [min max] [min max seed] :since "4.0"]
  [unix-date [e]]
  [unix-micros [e]]
  [unix-millis [e]]
  [unix-seconds [e]]
  [unix-timestamp [] [s] [s p]]
  [unwrap-udt [column]]
  [upper [e]]
  [url-decode [str]]
  [url-encode [str]]
  [user []]
  [uuid [] [seed] :arity-since {1 "4.1"}]
  [validate-utf8 [str] :since "4.0"]
  [var-pop [e]]
  [variance [e]]
  [variant-get [v path target-type] :since "4.0"]
  [weekday [e]]
  [weekofyear [e]]
  [width-bucket [v min max num-bucket]]
  [window [time-column window-duration] [time-column window-duration slide-duration] [time-column window-duration slide-duration start-time]]
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
  [xxhash64 [& cols]]
  [year [e]]
  [years [e]]
  [zeroifnull [col] :since "4.0"])

;; Aliases
(import-fn atan2 atan-2)
(import-fn base64 base-64)
(import-fn cbrt cube-root)
(import-fn covar-samp covar)
(import-fn crc32 crc-32)
(import-fn datediff date-diff)
(import-fn dayofmonth day-of-month)
(import-fn dayofweek day-of-week)
(import-fn dayofyear day-of-year)
(import-fn expm1 expm-1)
(import-fn log10 log-10)
(import-fn log1p log-1p)
(import-fn log2 log-2)
(import-fn md5 md-5)
(import-fn not !)
(import-fn pow **)
(import-fn sha1 sha-1)
(import-fn sha2 sha-2)
(import-fn shiftleft shift-left)
(import-fn shiftright shift-right)
(import-fn shiftrightunsigned shift-right-unsigned)
(import-fn stddev std)
(import-fn stddev stddev-samp)
(import-fn to-date ->date-col)
(import-fn to-timestamp ->timestamp-col)
(import-fn to-utc-timestamp ->utc-timestamp)
(import-fn unbase64 unbase-64)
(import-fn variance var-samp)
(import-fn weekofyear week-of-year)
(import-fn window time-window)
(import-fn xxhash64 xxhash-64)
