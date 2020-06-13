(ns zero-one.geni.sql
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
                            flatten
                            hash
                            last
                            mod
                            not
                            rand
                            reverse
                            second
                            sequence
                            shuffle
                            struct
                            when])
  (:require
    [zero-one.geni.column :refer [->col-array ->column]]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql Column functions)))

;; TODO: map-from-arrays, map-from-entries, map-keys, map-values, map
;; TODO: schema-of-json, from-json, to-json

;;;; Agg Functions
(defn approx-count-distinct
  ([expr] (functions/approx_count_distinct (->column expr)))
  ([expr rsd] (functions/approx_count_distinct (->column expr) rsd)))
(defn count-distinct [& exprs]
  (let [[head & tail] (map ->column exprs)]
    (functions/countDistinct head (into-array Column tail))))
(defn cume-dist [] (functions/cume_dist))
(defn dense-rank [] (functions/dense_rank))
(defn grouping [expr] (functions/grouping (->column expr)))
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
(defn spark-partition-id [] (functions/spark-partition-id))

;;;; Arithmetic Functions
(def pi (functions/lit Math/PI))
(defn abs [expr] (functions/abs (->column expr)))
(defn acos [expr] (functions/acos (->column expr)))
(defn asin [expr] (functions/asin (->column expr)))
(defn atan [expr] (functions/atan (->column expr)))
(defn atan2 [expr-x expr-y] (functions/atan2 (->column expr-x) (->column expr-y)))
(defn bround [expr] (functions/bround (->column expr)))
(defn ceil [expr] (functions/ceil (->column expr)))
(defn cbrt [expr] (functions/cbrt (->column expr)))
(def cube-root cbrt)
(defn cos [expr] (functions/cos (->column expr)))
(defn cosh [expr] (functions/cosh (->column expr)))
(defn exp [expr] (functions/exp (->column expr)))
(defn floor [expr] (functions/floor (->column expr)))
(defn hypot [left-expr right-expr] (functions/hypot (->column left-expr) (->column right-expr)))
(defn log [expr] (functions/log (->column expr)))
(defn log1p [expr] (functions/log1p (->column expr)))
(defn log2 [expr] (functions/log2 (->column expr)))
(defn nanvl [left-expr right-expr] (functions/nanvl (->column left-expr) (->column right-expr)))
(defn negate [expr] (functions/negate (->column expr)))
(defn pow [base exponent] (functions/pow (->column base) exponent))
(defn pmod [left-expr right-expr] (functions/pmod (->column left-expr) (->column right-expr)))
(defn rint [expr] (functions/rint (->column expr)))
(defn round [expr] (functions/round (->column expr)))
(defn sin [expr] (functions/sin (->column expr)))
(defn sinh [expr] (functions/sinh (->column expr)))
(defn sqr [expr] (.multiply (->column expr) (->column expr)))
(defn sqrt [expr] (functions/sqrt (->column expr)))
(defn tan [expr] (functions/tan (->column expr)))
(defn tanh [expr] (functions/tanh (->column expr)))

;;;; Array Functions
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
(defn array [exprs]
  (functions/array (->col-array exprs)))
(defn arrays-overlap [left right]
  (functions/arrays_overlap (->column left) (->column right)))
(defn arrays-zip [exprs]
  (functions/arrays_zip (->col-array exprs)))
(defn collect-list [expr] (functions/collect_list expr))
(defn collect-set [expr] (functions/collect_set expr))
(defn explode [expr] (functions/explode (->column expr)))
(def explode-outer explode)
(defn element-at [expr value]
  (functions/element_at (->column expr) (int value)))
(defn flatten [expr]
  (functions/flatten (->column expr)))
(defn last [expr] (functions/last (->column expr)))
(defn posexplode [expr] (functions/posexplode (->column expr)))
(def posexplode-outer posexplode)
(defn reverse [expr]
  (functions/reverse (->column expr)))
(defn sequence [start stop step]
  (functions/sequence (->column start) (->column stop) (->column step)))
(defn shuffle [expr]
  (functions/shuffle (->column expr)))
(defn size [expr]
  (functions/size (->column expr)))
(defn slice [expr start length]
  (functions/slice (->column expr) start length))
(defn sort-array
  ([expr] (functions/sort_array (->column expr)))
  ([expr asc] (functions/sort_array (->column expr) asc)))
(defn struct [& exprs] (functions/struct (->col-array exprs)))

;;;; Number Functions
(defn base64 [expr] (functions/base64 (->column expr)))
(defn bin [expr] (functions/bin (->column expr)))
(defn bitwise-not [expr] (functions/bitwiseNOT (->column expr)))
(defn conv [expr from-base to-base] (functions/conv (->column expr) from-base to-base))
(defn crc32 [expr] (functions/crc32 (->column expr)))
(defn decode [expr charset] (functions/decode (->column expr) charset))
(defn degrees [expr] (functions/degrees (->column expr)))
(defn encode [expr charset] (functions/encode (->column expr) charset))
(defn factorial [expr] (functions/factorial (->column expr)))
(defn hex [expr] (functions/hex (->column expr)))
(defn radians [expr] (functions/radians (->column expr)))
(defn signum [expr] (functions/signum (->column expr)))
(def sign signum)
(defn shift-left [expr num-bits] (functions/shiftLeft (->column expr) num-bits))
(defn shift-right [expr num-bits] (functions/shiftRight (->column expr) num-bits))
(defn shift-right-unsigned [expr num-bits]
  (functions/shiftRightUnsigned (->column expr) num-bits))
(defn unbase64 [expr] (functions/unbase64 (->column expr)))
(defn unhex [expr] (functions/unhex (->column expr)))

;;;; Boolean Functions
(defn not [expr] (functions/not (->column expr)))
(defn when
  ([condition if-expr]
   (functions/when condition (->column if-expr)))
  ([condition if-expr else-expr]
   (-> (when condition if-expr) (.otherwise (->column else-expr)))))

;;;; Dataset Functions
(defn broadcast [dataframe] (functions/broadcast dataframe))

;;;; Hash Functions
(defn hash [& exprs] (functions/hash (->col-array exprs)))
(defn md5 [expr] (functions/md5 (->column expr)))
(defn sha1 [expr] (functions/sha1 (->column expr)))
(defn sha2 [expr n-bits] (functions/sha2 (->column expr) n-bits))

;;;; Random Functions
(defn randn
  ([] (functions/randn))
  ([seed] (functions/randn seed)))
(defn rand
  ([] (functions/rand))
  ([seed] (functions/rand seed)))

;;;; Stats Functions
(defn corr [l-expr r-expr]
  (functions/corr (->column l-expr) (->column r-expr)))
(defn covar [l-expr r-expr]
  (functions/covar_samp (->column l-expr) (->column r-expr)))
(def covar-samp covar)
(defn covar-pop [l-expr r-expr] (functions/covar_pop (->column l-expr) (->column r-expr)))
(defn kurtosis [expr] (functions/kurtosis expr))
(defn lit [expr] (functions/lit expr))
(defn skewness [expr] (functions/skewness expr))
(defn stddev [expr] (functions/stddev expr))
(def stddev-samp stddev)
(defn stddev-pop [expr] (functions/stddev_pop expr))
(defn sum-distinct [expr] (functions/sumDistinct (->column expr)))
(defn var-pop [expr] (functions/var_pop (->column expr)))
(defn variance [expr] (functions/variance expr))
(def var-samp variance)

;;;; String Functions
(defn ascii [expr] (functions/ascii (->column expr)))
(defn concat [& exprs] (functions/concat (->col-array exprs)))
(defn concat-ws [sep & exprs] (functions/concat_ws sep (->col-array exprs)))
(defn expr [s] (functions/expr s))
(defn format-number [expr decimal-places]
  (functions/format_number (->column expr) decimal-places))
(defn format-string [fmt exprs]
  (functions/format_string fmt (->col-array exprs)))
(defn initcap [expr] (functions/initcap (->column expr)))
(defn instr [expr substr] (functions/instr (->column expr) substr))
(defn locate [substr expr] (functions/locate substr (->column expr)))
(defn lower [expr] (functions/lower (->column expr)))
(defn lpad [expr length pad] (functions/lpad (->column expr) length pad))
(defn ltrim [expr] (functions/ltrim (->column expr)))
(defn regexp-extract [expr regex idx]
  (functions/regexp_extract (->column expr) regex idx))
(defn regexp-replace [expr pattern-expr replacement-expr]
  (functions/regexp_replace
    (->column expr)
    (->column pattern-expr)
    (->column replacement-expr)))
(defn rpad [expr length pad] (functions/rpad (->column expr) length pad))
(defn rtrim [expr] (functions/rtrim (->column expr)))
(defn split [expr pattern] (functions/split (->column expr) pattern))
(defn substring [expr pos len] (functions/substring (->column expr) pos len))
(defn translate [expr match replacement]
  (functions/translate (->column expr) match replacement))
(defn trim [expr trim-string] (functions/trim (->column expr) trim-string))
(defn upper [expr] (functions/upper (->column expr)))

;;;; Time Functions
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
(def date-diff datediff)
(defn day-of-month [expr] (functions/dayofmonth (->column expr)))
(defn day-of-week [expr] (functions/dayofweek (->column expr)))
(defn day-of-year [expr] (functions/dayofyear (->column expr)))
(defn from-unixtime [expr] (functions/from_unixtime (->column expr)))
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
(defn to-date [expr date-format]
  (functions/to_date (->column expr) date-format))
(def ->date-col to-date)
(defn to-timestamp [expr]
  (functions/to_timestamp (->column expr)))
(def ->timestamp-col to-timestamp)
(defn to-utc-timestamp [expr]
  (functions/to_timestamp (->column expr)))
(def ->utc-timestamp to-utc-timestamp)
(defn unix-timestamp
  ([] (functions/unix_timestamp))
  ([expr] (functions/unix_timestamp (->column expr)))
  ([expr pattern] (functions/unix_timestamp (->column expr) pattern)))
(defn week-of-year [expr] (functions/weekofyear (->column expr)))
(defn year [expr] (functions/year (->column expr)))

;;;; Other Functions
(defn input-file-name [] (functions/input_file_name))
(defn monotonically-increasing-id [] (functions/monotonically_increasing_id))

;;;; Column Methods
;; Basics
(defn cast [expr new-type] (.cast (->column expr) new-type))

;; Booleans
(defn && [& exprs] (reduce #(.and (->column %1) (->column %2)) (lit true) exprs))
(defn || [& exprs] (reduce #(.or (->column %1) (->column %2)) (lit false) exprs))
(defn- compare-columns [compare-fn expr-0 & exprs]
  (let [exprs (-> exprs (conj expr-0))]
    (reduce
      (fn [acc-col [l-expr r-expr]]
        (&& acc-col (compare-fn (->column l-expr) (->column r-expr))))
      (lit true)
      (map vector exprs (rest exprs)))))
(def < (partial compare-columns #(.lt %1 %2)))
(def <= (partial compare-columns #(.leq %1 %2)))
(def === (partial compare-columns #(.equalTo %1 %2)))
(def > (partial compare-columns #(.gt %1 %2)))
(def >= (partial compare-columns #(.geq %1 %2)))
(defn between [expr lower-bound upper-bound]
  (.between (->column expr) lower-bound upper-bound))
(defn greatest [& exprs] (functions/greatest (->col-array exprs)))
(defn isin [expr coll] (.isin (->column expr) (interop/->scala-seq coll)))
(defn least [& exprs] (functions/least (->col-array exprs)))

;; Arithmetic
(defn + [& exprs] (reduce #(.plus (->column %1) (->column %2)) (lit 0) exprs))
(defn - [& exprs] (reduce #(.minus (->column %1) (->column %2)) exprs))
(defn * [& exprs] (reduce #(.multiply (->column %1) (->column %2)) (lit 1) exprs))
(defn / [& exprs] (reduce #(.divide (->column %1) (->column %2)) exprs))
(defn mod [left-expr right-expr] (.mod (->column left-expr) (->column right-expr)))

;; Missing Data
(defn nan? [expr] (.isNaN (->column expr)))
(defn null? [expr] (.isNull (->column expr)))
(defn null-rate [expr]
  (-> expr null? (cast "int") functions/mean (.as (str "null_rate(" expr ")"))))
(defn null-count [expr]
  (-> expr null? (cast "int") functions/sum (.as (str "null_count(" expr ")"))))

;; Strings
(defn contains [expr literal] (.contains (->column expr) literal))
(defn ends-with [expr literal] (.endsWith (->column expr) literal))
(defn length [expr] (functions/length (->column expr)))
(defn levenshtein [left-expr right-expr]
  (functions/levenshtein (->column left-expr) (->column right-expr)))
(defn like [expr literal] (.like (->column expr) literal))
(defn rlike [expr literal] (.rlike (->column expr) literal))
(defn starts-with [expr literal] (.startsWith (->column expr) literal))

;; Sorting
(defn asc [expr] (.asc (->column expr)))
(defn asc-nulls-first [expr] (.asc_nulls_first (->column expr)))
(defn asc-nulls-last [expr] (.asc_nulls_last (->column expr)))
(defn desc [expr] (.desc (->column expr)))
(defn desc-nulls-first [expr] (.desc_nulls_first (->column expr)))
(defn desc-nulls-last [expr] (.desc_nulls_last (->column expr)))
