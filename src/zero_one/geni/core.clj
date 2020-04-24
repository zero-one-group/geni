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
                            group-by
                            map
                            max
                            min
                            not
                            partition-by
                            second
                            take
                            when])
  (:import
    (scala.collection JavaConversions Map)
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark.sql Column Dataset functions)
    (org.apache.spark.sql SparkSession)
    (org.apache.spark.sql.expressions Window)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn scala-seq->vec [scala-seq]
  (into [] (JavaConversions/seqAsJavaList scala-seq)))

(defn scala-map->map [^Map m]
  (into {} (JavaConversions/mapAsJavaMap m)))

(defn ->scala-seq [coll]
  (JavaConversions/asScalaBuffer (seq coll)))

(defn scala-tuple->vec [p]
  (->> (.productArity p)
       (range)
       (clojure.core/map #(.productElement p %))
       (into [])))

(defn read-parquet! [spark-session path]
  (.. spark-session
      read
      (parquet path)))

(defn write-parquet! [dataframe path]
  (.. dataframe
      write
      (mode "overwrite")
      (parquet path)))

(defn write-csv! [dataframe path]
  (.. dataframe
      write
      (format "com.databricks.spark.csv")
      (option "header" "true")
      (mode "overwrite")
      (save path)))

(defn read-csv! [spark-session path]
  (.. spark-session
      read
      (format "csv")
      (option "header" "true")
      (load path)))

(defmulti col class)
(defmethod col org.apache.spark.sql.Column [x] x)
(defmethod col java.lang.String [x] (functions/col x))
(defn ->col-array [columns]
  (->> columns (clojure.core/map col) (into-array Column)))
(def ->column col)

(defn explain [dataframe] (.explain dataframe)) ;; TODO: test

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

(defn empty? [dataframe] (.isEmpty dataframe))

(defn repartition [dataframe & args]
  (let [args          (flatten args)
        [head & tail] (flatten args)]
    (if (int? head)
      (.repartition dataframe head (->col-array tail))
      (.repartition dataframe (->col-array args)))))
(defn repartition-by-range [dataframe & args]
  (let [args          (flatten args)
        [head & tail] (flatten args)]
    (if (int? head)
      (.repartitionByRange dataframe head (->col-array tail))
      (.repartitionByRange dataframe (->col-array args))))) ;; TODO: test
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
    (into {} (clojure.core/map scala-tuple->vec dtypes-as-tuples))))

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

(defn === [left-expr right-expr]
  (.equalTo (->column left-expr) (->column right-expr)))

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
  ([grouped expr values] (.pivot grouped (->column expr) (->scala-seq values))))

(defn agg [dataframe & exprs]
  (let [[head & tail] (clojure.core/map ->column (flatten exprs))]
    (.agg dataframe head (into-array Column tail))))

(defn cache [dataframe] (.cache dataframe))
(defn persist [dataframe] (.persist dataframe))

(defn as [column new-name] (.as column new-name))
(def alias as)
(defn cast [expr new-type] (.cast (->column expr) new-type))
(defn to-date [expr date-format]
  (functions/to_date (->column expr) date-format))
(def ->date-col to-date)

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
(defn year [expr] (functions/year (->column expr)))
(defn month [expr] (functions/month (->column expr)))
(defn day-of-year [expr] (functions/dayofyear (->column expr)))
(defn day-of-month [expr] (functions/dayofmonth (->column expr)))
(defn day-of-week [expr] (functions/dayofweek (->column expr)))
(defn hour [expr] (functions/hour (->column expr)))
(defn minute [expr] (functions/minute (->column expr)))
(defn second [expr] (functions/second (->column expr)))

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
(defn regexp-replace [expr pattern-expr replacement-expr]
  (functions/regexp_replace
    (->column expr)
    (->column pattern-expr)
    (->column replacement-expr)))
(defn regexp-extract [expr regex idx]
  (functions/regexp_extract (->column expr) regex idx))

(defn asc [expr] (.asc (->column expr)))
(defn desc [expr] (.desc (->column expr)))

(defn not [expr] (functions/not (->column expr)))
(defn log [expr] (functions/log (->column expr)))
(defn sqrt [expr] (functions/sqrt (->column expr)))
(defn pow [base exponent] (functions/pow (->column base) exponent))
(defn negate [expr] (functions/negate (->column expr)))
(defn abs [expr] (functions/abs (->column expr)))

(defn lit [expr] (functions/lit expr))
(defn min [expr] (functions/min expr))
(defn max [expr] (functions/max expr))
(defn stddev [expr] (functions/stddev expr))
(defn variance [expr] (functions/variance expr))
(defn mean [expr] (functions/mean expr))
(def avg mean)
(defn sum [expr] (functions/sum expr))

(defn + [left right] (.plus (->column left) (->column right)))
(defn - [left right] (.minus (->column left) (->column right)))
(defn * [left right] (.multiply (->column left) (->column right)))
(defn / [left right] (.divide (->column left) (->column right)))
(defn < [left right] (.lt (->column left) (->column right)))
(defn <= [left right] (.leq (->column left) (->column right)))
(defn > [left right] (.gt (->column left) (->column right)))
(defn >= [left right] (.geq (->column left) (->column right)))

(defn null? [expr] (.isNull (->column expr)))
(defn null-rate [expr]
  (-> expr null? (cast "int") mean (as (str "null_rate(" expr ")"))))
(defn null-count [expr]
  (-> expr null? (cast "int") sum (as (str "null_count(" expr ")"))))

(defn isin [expr coll] (.isin (->column expr) (->scala-seq coll)))

(defn substring [expr pos len] (functions/substring (->column expr) pos len))

(defn collect-list [expr] (functions/collect_list expr))
(defn collect-set [expr] (functions/collect_set expr))
(defn explode [expr] (functions/explode (->column expr)))

(defn when
  ([condition if-expr]
   (functions/when condition (->column if-expr)))
  ([condition if-expr else-expr]
   (-> (when condition if-expr) (.otherwise (->column else-expr)))))

(defmulti coalesce (fn [head & _] (class head)))
(defmethod coalesce Column [& exprs]
  (functions/coalesce (->col-array exprs))) ;; TODO: test
(defmethod coalesce Dataset [dataframe n-partitions]
  (.coalesce dataframe n-partitions)) ;; TODO: test

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

(defn spark-partition-id [] (functions/spark-partition-id))
(defn row-number [] (functions/row_number))

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

(defn union [left-df right-df] (.union left-df right-df))
(defn union-by-name [left-df right-df] (.unionByName left-df right-df))

(defn collect [dataframe]
  (let [spark-rows (.collect dataframe)
        col-names  (column-names dataframe)]
    (for [row spark-rows]
      (into {} (clojure.core/map
                 vector
                 col-names
                 (-> row .toSeq scala-seq->vec))))))

(defn collect-vals [dataframe]
  (mapv #(into [] (vals %)) (collect dataframe)))

(defn take [dataframe n-rows] (-> dataframe (limit n-rows) collect))
(defn take-vals [dataframe n-rows] (-> dataframe (limit n-rows) collect-vals))

(defn first [dataframe] (-> dataframe (take 1) clojure.core/first))
(defn first-vals [dataframe] (-> dataframe (take-vals 1) clojure.core/first))

(defn join
  ([left right join-cols] (join left right join-cols "inner"))
  ([left right join-cols join-type]
   (let [join-cols (if (string? join-cols) [join-cols] join-cols)]
     (.join left right (->scala-seq join-cols) join-type))))

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
                       (fn [s [k v]] (.config s k v)) ;; TODO: test
                       unconfigured
                       configs)
        session      (.getOrCreate configured)
        context      (.sparkContext session)]
    (.setLogLevel context log-level)
    session))

(defonce spark
  (delay (create-spark-session {"spark.testing.memory" "2147480000"})))

(defonce dataframe
  (delay
    (cache (read-parquet!
             @spark
             "test/resources/melbourne_housing_snapshot.parquet"))))

(comment

  (-> @dataframe count)

  (-> @dataframe print-schema)

  ;; TODO:
  ;; SQL: add_months, date_add (+ add_days), date_diff, months_between, next_day
  ;; current_timestamp, current_date, last_day, weekofyear
  ;; ceil, floor, exp, log, round
  ;; covar_samp (+ covar), kurtosis, skewness, rand, randn
  ;; sin, cos, tan, asin, acos, atan, atan2, sinh, cosh, tanh

  ;; TODO: Clojure docs
  ;; TODO: data-driven query

  0)
