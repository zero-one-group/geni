(ns zero-one.geni.core.dataset
  (:refer-clojure :exclude [distinct
                            drop
                            empty?
                            group-by
                            sort
                            take])
  (:require
   [clojure.walk :refer [keywordize-keys]]
   [potemkin :refer [import-fn]]
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (org.apache.spark.sql Column Dataset)
    (org.apache.spark.sql.types StructType)))

;;;; Actions
(defn- collected->maps [collected]
  (map interop/->clojure collected))

(defn- collected->vectors [collected cols]
  (map (apply juxt cols) (collected->maps collected)))

(defn collect [dataframe]
  (->> dataframe .collect collected->maps))

(defn head
  ([dataframe] (-> dataframe (.head 1) collected->maps first))
  ([dataframe n-rows] (-> dataframe (.head n-rows) collected->maps)))

(defn describe [dataframe & col-names]
  (.describe dataframe (into-array java.lang.String (map name col-names))))

(defn tail [dataframe n-rows]
  (-> dataframe (.tail n-rows) collected->maps))

(defn take [dataframe n-rows]
  (-> dataframe (.take n-rows) collected->maps))

(defn schema ^StructType
  [^Dataset dataframe]
  (.schema dataframe))

(defn show
  ([dataframe] (show dataframe {}))
  ([dataframe options]
   (let [{:keys [num-rows truncate vertical]
          :or   {num-rows 20
                 truncate 0
                 vertical false}} options]
     (-> dataframe (.showString num-rows truncate vertical) println))))

(defn summary [dataframe & stat-names]
  (.summary dataframe (into-array java.lang.String (map name stat-names))))

;; Basic
(defn cache [dataframe] (.cache dataframe))

(defn checkpoint
  ([dataframe] (.checkpoint dataframe true))
  ([dataframe eager] (.checkpoint dataframe eager)))

(defn columns
  "Returns all column names as an array of keywords."
  [dataframe]
  (->> dataframe .columns seq (map keyword)))

(defn dtypes [dataframe]
  (let [dtypes-as-tuples (-> dataframe .dtypes seq)]
    (->> dtypes-as-tuples
         (map interop/scala-tuple->vec)
         (into {})
         keywordize-keys)))

(defn input-files [dataframe] (seq (.inputFiles dataframe)))

(defn is-empty [dataframe] (.isEmpty dataframe))

(defn is-local [dataframe] (.isLocal dataframe))

(defn persist
  ([dataframe] (.persist dataframe))
  ([dataframe new-level] (.persist dataframe new-level)))

(defn print-schema [dataframe]
  (-> dataframe .schema .treeString println))

(defn rdd [dataframe] (.rdd dataframe))

(defn storage-level [dataframe] (.storageLevel dataframe))

(defn unpersist
  ([dataframe] (.unpersist dataframe))
  ([dataframe blocking] (.unpersist dataframe blocking)))

;;;; Streaming
(defn is-streaming [dataframe] (.isStreaming dataframe))

;;;; Typed Transformations
(defn distinct [dataframe] (.distinct dataframe))

(defn drop-duplicates [dataframe & col-names]
  (if (clojure.core/empty? col-names)
    (.dropDuplicates dataframe)
    (.dropDuplicates dataframe (into-array java.lang.String (map name col-names)))))

(defn except [dataframe other] (.except dataframe other))

(defn except-all [dataframe other] (.exceptAll dataframe other))

(defn intersect [dataframe other] (.intersect dataframe other))

(defn intersect-all [dataframe other] (.intersectAll dataframe other))

(defn join-with
  ([left right condition] (.joinWith left right condition))
  ([left right condition join-type] (.joinWith left right condition join-type)))

(defn limit [dataframe n-rows] (.limit dataframe n-rows))

(defn order-by [dataframe & exprs] (.orderBy dataframe (->col-array exprs)))

(defn partitions [dataframe]
  (seq (.. dataframe rdd partitions)))

(defn random-split
  ([dataframe weights] (.randomSplit dataframe (double-array weights)))
  ([dataframe weights seed] (.randomSplit dataframe (double-array weights) seed)))

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
      (.repartitionByRange dataframe (->col-array args)))))

(defn sample
  ([dataframe fraction] (.sample dataframe fraction))
  ([dataframe fraction with-replacement]
   (.sample dataframe with-replacement fraction)))

(defn sort-within-partitions [dataframe & exprs]
  (.sortWithinPartitions dataframe (->col-array exprs)))

(defn union [& dataframes] (reduce #(.union %1 %2) dataframes))

(defn union-by-name [& dataframes] (reduce #(.unionByName %1 %2) dataframes))

;; Untyped Transformations
(defn agg [dataframe & args]
  (let [[head & tail] (->col-array args)]
    (.agg dataframe head (into-array Column tail))))

(defn agg-all
  "Aggregates on all columns of the entire Dataset without groups."
  [dataframe agg-fn]
  (let [agg-cols (map agg-fn (-> dataframe .columns seq))]
    (apply agg dataframe agg-cols)))

(defn col-regex [dataframe col-name] (.colRegex dataframe (name col-name)))

(defn cross-join [left right] (.crossJoin left right))

(defn cube [dataframe & exprs]
  (.cube dataframe (->col-array exprs)))

(defn drop [dataframe & col-names]
  (let [flattened (mapcat ensure-coll col-names)]
    (.drop dataframe (into-array java.lang.String (map name flattened)))))

(defn group-by [dataframe & exprs]
  (.groupBy dataframe (->col-array exprs)))

(defn- ->join-expr-or-join-cols [expr]
  (if (instance? Column expr)
    expr
    (->> (ensure-coll expr)
         (map name)
         interop/->scala-seq)))

(defn join
  ([left right expr] (join left right expr "inner"))
  ([left right expr join-type]
   (.join left right (->join-expr-or-join-cols expr) join-type)))

(defn rollup [dataframe & exprs]
  (.rollup dataframe (->col-array exprs)))

(defn select [dataframe & exprs] (.select dataframe (->col-array exprs)))

(defn select-expr [dataframe & exprs]
  (.selectExpr dataframe (into-array java.lang.String exprs)))

(defn with-column [dataframe col-name expr]
  (.withColumn dataframe (name col-name) (->column expr)))

(defn with-column-renamed [dataframe old-name new-name]
  (.withColumnRenamed dataframe (name old-name) (name new-name)))

;;;; Ungrouped
(defn spark-session [dataframe] (.sparkSession dataframe))

(defn sql-context [dataframe] (.sqlContext dataframe))

;;;; Relational Grouped Dataset
(defn pivot
  ([grouped expr] (.pivot grouped (->column expr)))
  ([grouped expr values] (.pivot grouped (->column expr) (interop/->scala-seq values))))

;; Stat Functions
(defn approx-quantile [dataframe col-or-cols probs rel-error]
  (let [seq-col     (coll? col-or-cols)
        col-or-cols (if seq-col
                      (into-array java.lang.String (map name col-or-cols))
                      (name col-or-cols))
        quantiles   (-> dataframe
                        .stat
                        (.approxQuantile col-or-cols (double-array probs) rel-error))]
    (if seq-col
      (map seq quantiles)
      (seq quantiles))))

(defn bloom-filter [dataframe expr expected-num-items num-bits-or-fpp]
  (-> dataframe
      .stat
      (.bloomFilter (->column expr) expected-num-items num-bits-or-fpp)))
(defn bit-size [bloom] (.bitSize bloom))
(defn expected-fpp [bloom] (.expectedFpp bloom))
(defn is-compatible [bloom other] (.isCompatible bloom other))
(defn might-contain [bloom item] (.mightContain bloom item))
(defn put [bloom item] (.put bloom item))

(defn count-min-sketch [dataframe expr eps-or-depth confidence-or-width seed]
  (-> dataframe .stat (.countMinSketch (->column expr) eps-or-depth confidence-or-width seed)))
(defn add
  ([cms item] (.add cms item))
  ([cms item cnt] (.add cms item cnt)))
(defn confidence [cms] (.confidence cms))
(defn depth [cms] (.depth cms))
(defn estimate-count [cms item] (.estimateCount cms item))
(defn relative-error [cms] (.relativeError cms))
(defn to-byte-array [cms] (.toByteArray cms))
(defn total-count [cms] (.totalCount cms))
(defn width [cms] (.width cms))

(defn cov [dataframe col-name1 col-name2]
  (-> dataframe .stat (.cov (name col-name1) (name col-name2))))

(defn crosstab [dataframe col-name1 col-name2]
  (-> dataframe .stat (.crosstab (name col-name1) (name col-name2))))

(defn freq-items
  ([dataframe col-names]
   (-> dataframe .stat (.freqItems (interop/->scala-seq (map name col-names)))))
  ([dataframe col-names support]
   (-> dataframe .stat (.freqItems (interop/->scala-seq (map name col-names)) support))))

(defn merge-in-place [bloom-or-cms other] (.mergeInPlace bloom-or-cms other))

(defn sample-by [dataframe expr fractions seed]
  (let [casted-fractions (->> fractions
                              (map (fn [[row-seq frac]]
                                     [(interop/->spark-row row-seq) frac]))
                              (into {}))]
    (-> dataframe .stat (.sampleBy (->column expr) casted-fractions seed))))

;; NA Functions
(defn drop-na
  ([dataframe]
   (-> dataframe .na .drop))
  ([dataframe min-non-nulls-or-cols]
   (if (coll? min-non-nulls-or-cols)
     (-> dataframe .na (.drop (interop/->scala-seq (map name min-non-nulls-or-cols))))
     (-> dataframe .na (.drop min-non-nulls-or-cols))))
  ([dataframe min-non-nulls cols]
   (-> dataframe .na (.drop min-non-nulls (interop/->scala-seq (map name cols))))))

(defn fill-na
  ([dataframe value]
   (-> dataframe .na (.fill value)))
  ([dataframe value cols]
   (-> dataframe .na (.fill value (interop/->scala-seq (map name cols))))))

(defn replace-na [dataframe cols replacement]
  (let [cols (map name (ensure-coll cols))]
    (-> dataframe
        .na
        (.replace (into-array java.lang.String cols)
                  (java.util.HashMap. replacement)))))

;;;; Convenience Functions
;; Actions
(defn collect-vals
  "Returns the vector values of the Dataset collected."
  [dataframe]
  (let [cols (columns dataframe)]
    (-> dataframe .collect (collected->vectors cols))))

(defn head-vals
  "Returns the vector values of the first n rows in the Dataset collected."
  ([dataframe]
   (let [cols (columns dataframe)]
     (-> dataframe (.head 1) (collected->vectors cols) first)))
  ([dataframe n-rows]
   (let [cols (columns dataframe)]
     (-> dataframe (.head n-rows) (collected->vectors cols)))))

(defn take-vals
  "Returns the vector values of the first n rows in the Dataset collected."
  [dataframe n-rows]
  (let [cols (columns dataframe)]
    (-> dataframe (.take n-rows) (collected->vectors cols))))

(defn tail-vals
  "Returns the vector values of the last n rows in the Dataset collected."
  [dataframe n-rows]
  (let [cols (columns dataframe)]
    (-> dataframe (.tail n-rows) (collected->vectors cols))))

(defn collect-col
  "Returns a vector that contains all rows in the column of the Dataset."
  [dataframe col-name]
  (map (keyword col-name) (-> dataframe (select col-name) collect)))

(defn first-vals
  "Returns the vector values of the first row in the Dataset collected."
  [dataframe]
  (-> dataframe (take-vals 1) first))

(defn last-vals
  "Returns the vector values of the last row in the Dataset collected."
  [dataframe]
  (-> dataframe (tail-vals 1) first))

;; Basic
(defn show-vertical
  "Displays the Dataset in a list-of-records form."
  ([dataframe] (show dataframe {:vertical true}))
  ([dataframe options] (show dataframe (assoc options :vertical true))))

(defn column-names
  "Returns all column names as an array of strings."
  [dataframe]
  (-> dataframe .columns seq))

(defn hint [dataframe hint-name & args]
  (.hint dataframe hint-name (interop/->scala-seq args)))

(defn rename-columns
  "Returns a new Dataset with a column renamed according to the rename-map."
  [dataframe rename-map]
  (reduce
   (fn [acc-df [old-name new-name]]
     (.withColumnRenamed acc-df (name old-name) (name new-name)))
   dataframe
   rename-map))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.core.dataset
 [(-> docs/spark-docs :methods :core :dataset)
  (-> docs/spark-docs :methods :core :grouped)
  (-> docs/spark-docs :methods :core :na-fns)
  (-> docs/spark-docs :methods :core :stat-fns)
  (-> docs/spark-docs :methods :util :bloom)
  (-> docs/spark-docs :methods :util :cms)])

(docs/add-doc!
 (var partitions)
 (-> docs/spark-docs :methods :rdd :rdd :partitions))

(docs/add-doc!
 (var drop-na)
 (-> docs/spark-docs :methods :core :na-fns :drop))

(docs/add-doc!
 (var fill-na)
 (-> docs/spark-docs :methods :core :na-fns :fill))

(docs/add-doc!
 (var replace-na)
 (-> docs/spark-docs :methods :core :na-fns :replace))

;; Aliases
(import-fn is-local local?)
(import-fn is-empty empty?)
(import-fn is-streaming streaming?)
(import-fn order-by sort)
(import-fn is-compatible compatible?)

