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
    [zero-one.geni.dataset]
    [zero-one.geni.data-sources]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (org.apache.spark.sql Column Dataset functions)
    (org.apache.spark.sql SparkSession)
    (org.apache.spark.sql.expressions Window)))

(defmulti col class)
(defmethod col :default [x] (functions/lit x))
(defmethod col org.apache.spark.sql.Column [x] x)
(defmethod col java.lang.String [x] (functions/col x))
(defn ->col-array [columns]
  (->> columns (clojure.core/map col) (into-array Column)))
(def ->column col)

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

(defn random-split [dataframe weights]
  (.randomSplit dataframe (double-array weights)))

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

(defn broadcast [dataframe] (functions/broadcast dataframe))
(defn cache [dataframe] (.cache dataframe))
(defn persist [dataframe] (.persist dataframe))

(defn md5 [expr] (functions/md5 (->column expr)))
(defn sha1 [expr] (functions/sha1 (->column expr)))
(defn sha2 [expr n-bits] (functions/sha2 (->column expr) n-bits))

(defmulti count class)
(defmethod count org.apache.spark.sql.Column [x] (functions/count x))
(defmethod count java.lang.String [x] (functions/count x))
(defmethod count org.apache.spark.sql.Dataset [x] (.count x))

(defn unix-timestamp
  ([] (functions/unix_timestamp))
  ([expr] (functions/unix_timestamp (->column expr)))
  ([expr pattern] (functions/unix_timestamp (->column expr) pattern)))
(defn current-timestamp [] (functions/current_timestamp))
(defn current-date [] (functions/current_date))
(defn year [expr] (functions/year (->column expr)))
(defn quarter [expr] (functions/quarter (->column expr)))
(defn month [expr] (functions/month (->column expr)))
(defn week-of-year [expr] (functions/weekofyear (->column expr)))
(defn day-of-year [expr] (functions/dayofyear (->column expr)))
(defn day-of-month [expr] (functions/dayofmonth (->column expr)))
(defn day-of-week [expr] (functions/dayofweek (->column expr)))
(defn last-day [expr] (functions/last_day (->column expr)))
(defn hour [expr] (functions/hour (->column expr)))
(defn minute [expr] (functions/minute (->column expr)))
(defn second [expr] (functions/second (->column expr)))

(defn to-date [expr date-format]
  (functions/to_date (->column expr) date-format))
(def ->date-col to-date)
(defn add-months [expr months]
  (functions/add_months (->column expr) months))
(defn months-between [l-expr r-expr]
  (functions/months_between (->column l-expr) (->column r-expr)))
(defn next-day [expr day-of-week]
  (functions/next_day (->column expr) day-of-week))
(defn date-add [expr days]
  (functions/date_add (->column expr) days))
(defn date-sub [expr days]
  (functions/date_sub (->column expr) days))
(defn datediff [l-expr r-expr]
  (functions/datediff (->column l-expr) (->column r-expr)))
(def date-diff datediff)
(defn date-format [expr date-fmt]
  (functions/date_format (->column expr) date-fmt))

(defn expr [s] (functions/expr s))
(defn format-number [expr decimal-places]
  (functions/format_number (->column expr) decimal-places))
(defn format-string [fmt exprs]
  (functions/format_string fmt (->col-array exprs)))
(defn lower [expr] (functions/lower (->column expr)))
(defn upper [expr] (functions/upper (->column expr)))
(defn lpad [expr length pad] (functions/lpad (->column expr) length pad))
(defn rpad [expr length pad] (functions/rpad (->column expr) length pad))
(defn ltrim [expr] (functions/ltrim (->column expr)))
(defn rtrim [expr] (functions/rtrim (->column expr)))
(defn trim [expr trim-string] (functions/trim (->column expr) trim-string))
(defn split [expr pattern] (functions/split (->column expr) pattern))
(defn regexp-replace [expr pattern-expr replacement-expr]
  (functions/regexp_replace
    (->column expr)
    (->column pattern-expr)
    (->column replacement-expr)))
(defn regexp-extract [expr regex idx]
  (functions/regexp_extract (->column expr) regex idx))

(defn lit [expr] (functions/lit expr))
(defn min [expr] (functions/min expr))
(defn max [expr] (functions/max expr))
(defn stddev [expr] (functions/stddev expr))
(defn variance [expr] (functions/variance expr))
(defn mean [expr] (functions/mean expr))
(def avg mean)
(defn sum [expr] (functions/sum expr))
(defn skewness [expr] (functions/skewness expr))
(defn kurtosis [expr] (functions/kurtosis expr))
(defn covar [l-expr r-expr] (functions/covar_samp (->column l-expr) (->column r-expr)))

(defn randn
  ([] (functions/randn))
  ([seed] (functions/randn seed)))
(defn rand
  ([] (functions/rand))
  ([seed] (functions/rand seed)))

(defn not [expr] (functions/not (->column expr)))
(defn log [expr] (functions/log (->column expr)))
(defn exp [expr] (functions/exp (->column expr)))
(defn sqr [expr] (.multiply (->column expr) (->column expr)))
(defn sqrt [expr] (functions/sqrt (->column expr)))
(defn pow [base exponent] (functions/pow (->column base) exponent))
(defn negate [expr] (functions/negate (->column expr)))
(defn abs [expr] (functions/abs (->column expr)))
(defn sin [expr] (functions/sin (->column expr)))
(defn cos [expr] (functions/cos (->column expr)))
(defn tan [expr] (functions/tan (->column expr)))
(defn asin [expr] (functions/asin (->column expr)))
(defn acos [expr] (functions/acos (->column expr)))
(defn atan [expr] (functions/atan (->column expr)))
(defn sinh [expr] (functions/sinh (->column expr)))
(defn cosh [expr] (functions/cosh (->column expr)))
(defn tanh [expr] (functions/tanh (->column expr)))
(defn ceil [expr] (functions/ceil (->column expr)))
(defn floor [expr] (functions/floor (->column expr)))
(defn round [expr] (functions/round (->column expr)))
(def pi (lit Math/PI))

(defn asc [expr] (.asc (->column expr)))
(defn desc [expr] (.desc (->column expr)))

(defn hash [& exprs] (functions/hash (->col-array exprs)))
(defn like [expr literal] (.like (->column expr) literal))
(defn rlike [expr literal] (.rlike (->column expr) literal))
(defn contains [expr literal] (.contains (->column expr) literal))
(defn starts-with [expr literal] (.startsWith (->column expr) literal))
(defn ends-with [expr literal] (.endsWith (->column expr) literal))

(defn as [expr new-name] (.as (->column expr) new-name))
(def alias as)
(defn cast [expr new-type] (.cast (->column expr) new-type))

(defn && [& exprs] (reduce #(.and (->column %1) (->column %2)) (lit true) exprs))
(defn || [& exprs] (reduce #(.or (->column %1) (->column %2)) (lit false) exprs))

(defn + [& exprs] (reduce #(.plus (->column %1) (->column %2)) (lit 0) exprs))
(defn - [& exprs] (reduce #(.minus (->column %1) (->column %2)) exprs))
(defn * [& exprs] (reduce #(.multiply (->column %1) (->column %2)) (lit 1) exprs))
(defn / [& exprs] (reduce #(.divide (->column %1) (->column %2)) exprs))
(defn mod [left-expr right-expr] (.mod (->column left-expr) (->column right-expr)))
(defn compare-columns [compare-fn expr-0 & exprs]
  (let [exprs (-> exprs (conj expr-0))]
    (reduce
      (fn [acc-col [l-expr r-expr]]
        (&& acc-col (compare-fn (->column l-expr) (->column r-expr))))
      (lit true)
      (clojure.core/map vector exprs (rest exprs)))))
(def < (partial compare-columns #(.lt %1 %2)))
(def <= (partial compare-columns #(.leq %1 %2)))
(def > (partial compare-columns #(.gt %1 %2)))
(def >= (partial compare-columns #(.geq %1 %2)))
(defn between [expr lower-bound upper-bound]
  (.between (->column expr) lower-bound upper-bound))
(def === (partial compare-columns #(.equalTo %1 %2)))

(defn nan? [expr] (.isNaN (->column expr)))
(defn null? [expr] (.isNull (->column expr)))
(defn null-rate [expr]
  (-> expr null? (cast "int") mean (as (str "null_rate(" expr ")"))))
(defn null-count [expr]
  (-> expr null? (cast "int") sum (as (str "null_count(" expr ")"))))

(defn isin [expr coll] (.isin (->column expr) (interop/->scala-seq coll)))

(defn substring [expr pos len] (functions/substring (->column expr) pos len))

(defn collect-list [expr] (functions/collect_list expr))
(defn collect-set [expr] (functions/collect_set expr))
(defn explode [expr] (functions/explode (->column expr)))

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

(defn element-at [expr value]
  (functions/element_at (->column expr) (int value)))
(defn flatten [expr]
  (functions/flatten (->column expr)))
(defn reverse [expr]
  (functions/reverse (->column expr)))
(defn shuffle [expr]
  (functions/shuffle (->column expr)))
(defn size [expr]
  (functions/size (->column expr)))
(defn slice [expr start length]
  (functions/slice (->column expr) start length))
(defn sort-array
  ([expr] (functions/sort_array (->column expr)))
  ([expr asc] (functions/sort_array (->column expr) asc)))

(defn when
  ([condition if-expr]
   (functions/when condition (->column if-expr)))
  ([condition if-expr else-expr]
   (-> (when condition if-expr) (.otherwise (->column else-expr)))))

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
(defn spark-partition-id [] (functions/spark-partition-id))

(defn count-distinct [& exprs]
  (let [[head & tail] (clojure.core/map ->column exprs)]
    (functions/countDistinct head (into-array Column tail))))

(defn approx-count-distinct
  ([expr] (functions/approx_count_distinct (->column expr)))
  ([expr rsd] (functions/approx_count_distinct (->column expr) rsd)))

(defn concat [& exprs] (functions/concat (->col-array exprs)))

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

(defn first [dataframe] (-> dataframe (take 1) clojure.core/first))
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

  ;; TODO: Clojure docs
  ;; TODO: data-driven query
  (require '[clojure.reflect :as r])
  (->> (r/reflect (.stat melbourne-df))
       :members
       (clojure.core/filter #(= (:name %) 'approxQuantile))
       (mapv :parameter-types)
       ;(mapv println)
       pprint)

  0)
