(ns zero-one.geni.core.dataset
  (:refer-clojure :exclude [distinct
                            drop
                            empty?
                            group-by
                            sort
                            take])
  (:require
   [clojure.string :as string]
   [clojure.walk :refer [keywordize-keys]]
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [class-named ensure-coll import-fn]])
  (:import
   (clojure.lang Reflector)
   (org.apache.spark.sql Column Observation)
   (org.apache.spark.sql.types Metadata StructType)))

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

(defn show
  ([dataframe] (show dataframe {}))
  ([dataframe options]
   (let [{:keys [num-rows truncate vertical]
          :or   {num-rows 20
                 truncate 0
                 vertical false}} options]
     ;; Dataset.show prints to Scala's Console, and works over Spark Connect,
     ;; unlike showString.
     (print (interop/with-scala-out-str (.show dataframe num-rows truncate vertical)))
     (flush))))

(defn summary [dataframe & stat-names]
  (.summary dataframe (into-array java.lang.String (map name stat-names))))

;; Basic
(defn cache [dataframe] (.cache dataframe))

(defn checkpoint
  ([dataframe] (.checkpoint dataframe true))
  ([dataframe eager] (.checkpoint dataframe eager)))

(defn- spark-version
  "The major and minor version of the Dataset's Spark, such as [4 2]."
  [dataframe]
  (->> (.version (.sparkSession dataframe))
       (re-find #"^(\d+)\.(\d+)")
       rest
       (mapv parse-long)))

(defn local-checkpoint
  "Returns a checkpointed version of the Dataset, with its plan cut at this
  point, so that the work behind it isn't done again. Unlike `checkpoint`, it
  keeps the data in the executors' storage rather than in the checkpoint
  directory: quicker, and it needs no directory, but the data is lost when an
  executor is. It runs a job now unless `eager` is false. From Spark 4.0, it
  takes the storage level, such as `g/memory-only`, which is
  `g/memory-and-disk` otherwise. `release-checkpoint!` frees it.

  ```clojure
  (g/local-checkpoint expensive)
  (g/local-checkpoint expensive true g/memory-only)
  ```"
  ([dataframe] (.localCheckpoint dataframe))
  ([dataframe eager] (.localCheckpoint dataframe (boolean eager)))
  ([dataframe eager storage-level]
   (when (neg? (compare (spark-version dataframe) [4 0]))
     (throw (ex-info (str "local-checkpoint takes a storage level from Spark 4.0. This is Spark "
                          (.version (.sparkSession dataframe)) ".")
                     {:spark-version (.version (.sparkSession dataframe))})))
   (.localCheckpoint dataframe (boolean eager) storage-level)))

(defn- not-a-checkpoint! [dataframe]
  (throw (ex-info (str "release-checkpoint! takes a Dataset that checkpoint or local-checkpoint "
                       "returned, before any other transformation.")
                  {:dataframe dataframe})))

(defn- delete-checkpoint-file! [spark-session ^String file]
  ;; Hadoop's classes come with classic Spark, which is the only Spark that
  ;; gets here, but not with a Spark Connect client.
  (let [path (Reflector/invokeConstructor (class-named "org.apache.hadoop.fs.Path")
                                          (object-array [file]))
        fs   (.getFileSystem path (.. spark-session sparkContext hadoopConfiguration))]
    (.delete fs path true)))

(defn- release-classic-checkpoint! [dataframe]
  (let [logical  (.. dataframe queryExecution logical)
        rdd-plan (class-named "org.apache.spark.sql.execution.LogicalRDD")
        rdd      (if (and rdd-plan (instance? rdd-plan logical))
                   (.rdd logical)
                   (not-a-checkpoint! dataframe))
        file     (.getCheckpointFile rdd)]
    (when (.isDefined file)
      (delete-checkpoint-file! (.sparkSession dataframe) (.get file)))
    (.unpersist rdd true)))

(defn- release-connect-checkpoint! [dataframe]
  (let [root (.getRoot (.plan dataframe))]
    (when-not (.hasCachedRemoteRelation root)
      (not-a-checkpoint! dataframe))
    (let [relation-id (.getRelationId (.getCachedRemoteRelation root))
          cleaner     (try
                        (.cleaner (.sparkSession dataframe))
                        (catch IllegalArgumentException e
                          (throw (ex-info (str "This Spark Connect client has no SessionCleaner, "
                                               "which release-checkpoint! needs.")
                                          {}
                                          e))))]
      (.doCleanupCachedRemoteRelation cleaner relation-id))))

(defn release-checkpoint!
  "Frees what a Dataset from `checkpoint` or `local-checkpoint` holds: the
  blocks of a local checkpoint, and the files of a reliable one in the
  checkpoint directory. Over Spark Connect, the server lets go of the
  checkpoint, though a reliable checkpoint's files stay until its context
  cleaner removes them, as `spark.cleaner.referenceTracking.cleanCheckpoints`
  has it do. The Dataset can't be read afterwards. Releasing twice does
  nothing more. Over Spark Connect, it calls the client's `SessionCleaner`,
  which isn't public API."
  [dataframe]
  (if (spark/classic-session? (.sparkSession dataframe))
    (release-classic-checkpoint! dataframe)
    (release-connect-checkpoint! dataframe))
  nil)

(defmacro with-checkpoint
  "Binds each name to a checkpointed Dataset, as `with-open` does, runs the
  body, and then releases the checkpoints with `release-checkpoint!`, in
  reverse order, whether or not the body throws. So the body can query the
  checkpoint many times, but what it returns can't depend on reading it later.

  ```clojure
  (g/with-checkpoint [base (g/local-checkpoint expensive)]
    {:rows (g/count base)
     :big  (g/count (g/filter base (g/> :price 1e6)))})
  ```"
  [bindings & body]
  (when-not (and (vector? bindings) (even? (count bindings)))
    (throw (IllegalArgumentException.
            "with-checkpoint takes a vector of names and checkpointed Datasets.")))
  (if (clojure.core/empty? bindings)
    `(do ~@body)
    `(let [~(bindings 0) ~(bindings 1)]
       (try
         (with-checkpoint ~(subvec bindings 2) ~@body)
         (finally
           (release-checkpoint! ~(bindings 0)))))))

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

(defn tree-string
  "Returns the schema as the tree that `print-schema` prints. With `level`, the
  tree goes that many levels deep.

  ```clojure
  (g/tree-string (g/range 3))
  => \"root\\n |-- id: long (nullable = false)\\n\"
  ```"
  ([dataframe] (-> dataframe .schema .treeString))
  ([dataframe level] (-> dataframe .schema (.treeString (int level)))))

(defn print-schema
  ([dataframe] (println (tree-string dataframe)))
  ([dataframe level] (println (tree-string dataframe level))))

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
  "Returns a sample of about `fraction` of the rows, without replacement unless
  `with-replacement` is true, and with a random seed unless `seed` is given.
  The third argument is `with-replacement` when it's a boolean, and the seed
  otherwise.

  ```clojure
  (g/sample dataframe 0.1)
  (g/sample dataframe 0.1 42)
  (g/sample dataframe 0.1 true 42)
  ```"
  ([dataframe fraction] (.sample dataframe (double fraction)))
  ([dataframe fraction with-replacement-or-seed]
   (if (boolean? with-replacement-or-seed)
     (.sample dataframe with-replacement-or-seed (double fraction))
     (.sample dataframe (double fraction) (long with-replacement-or-seed))))
  ([dataframe fraction with-replacement seed]
   (.sample dataframe (boolean with-replacement) (double fraction) (long seed))))

(defn sort-within-partitions [dataframe & exprs]
  (.sortWithinPartitions dataframe (->col-array exprs)))

(defn union [& dataframes] (reduce #(.union %1 %2) dataframes))

(defn union-by-name
  "Returns the union of the dataframes' rows, matching their columns by name.
  A map of options can follow the dataframes. With `:allow-missing-columns`
  true, a column that some of them lack is null in their rows.

  ```clojure
  (g/union-by-name left right)
  (g/union-by-name left right {:allow-missing-columns true})
  ```"
  [& dataframes-and-options]
  (let [options    (when (map? (last dataframes-and-options)) (last dataframes-and-options))
        dataframes (cond-> dataframes-and-options options butlast)
        allow?     (boolean (:allow-missing-columns options))]
    (reduce #(.unionByName %1 %2 allow?) dataframes)))

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

(defn with-columns
  "Returns a new Dataset with columns added, or replaced where a column of the
  same name exists, from a map of names to columns, or a seq of name-column
  pairs. A value that isn't a column goes through `->column`, as in
  `with-column`. The new columns go at the end, in the map's order, so pass
  pairs for more than eight, where a Clojure map no longer keeps its order.

  ```clojure
  (g/with-columns dataframe {:price-k (g/* :price 0.001)
                             :big?    (g/> :rooms 3)})
  ```"
  [dataframe cols]
  (.withColumns dataframe (interop/->scala-list-map
                           (map (fn [[k v]] [(name k) (->column v)]) cols))))

(defn offset
  "Returns a new Dataset that skips the first `n-rows` rows. As with `limit`,
  which rows come first is only certain after `order-by`.

  ```clojure
  (-> dataframe (g/order-by :id) (g/offset 10) (g/limit 10))
  ```"
  [dataframe n-rows]
  (.offset dataframe (int n-rows)))

(defn unpivot
  "Turns columns into rows: for each row, a row per column in `values`, with
  the `ids` columns, a `variable-col` column that holds the column's name and
  a `value-col` column that holds its value. The `values` columns need a
  common type. Without `values`, it unpivots every column that isn't in `ids`.
  Also called `melt`.

  ```clojure
  (g/unpivot sales [:id] [:jan :feb] :month :amount)
  (g/unpivot sales :id :month :amount)
  ```"
  ([dataframe ids variable-col value-col]
   (.unpivot dataframe
             (->col-array (ensure-coll ids))
             (name variable-col)
             (name value-col)))
  ([dataframe ids values variable-col value-col]
   (.unpivot dataframe
             (->col-array (ensure-coll ids))
             (->col-array (ensure-coll values))
             (name variable-col)
             (name value-col))))

(defn- ->struct-type ^StructType [schema]
  (let [parsed (if (string? schema)
                 (dataset-creation/parse-ddl schema)
                 (dataset-creation/->schema schema))]
    (if (instance? StructType parsed)
      parsed
      (throw (ex-info (str "Expected a schema: a struct type, a map such as {:id :long}, "
                           "or a DDL string such as \"id BIGINT\". Got: " (pr-str schema))
                      {:schema schema})))))

(defn to
  "Returns a new Dataset with the columns of `schema`, in its order, with its
  types: Spark's `Dataset.to`. It matches columns by name, drops the ones that
  `schema` lacks, casts where a column's type differs and the cast is safe,
  and fills a missing nullable column with nulls. `schema` is a struct type, a
  map as for `->schema`, or a DDL string.

  ```clojure
  (g/to dataframe \"id BIGINT, name STRING\")
  (g/to dataframe {:id :long :name :string})
  ```"
  [dataframe schema]
  (.to dataframe (->struct-type schema)))

(defn- ->metadata ^Metadata [metadata]
  (if (instance? Metadata metadata)
    metadata
    (Metadata/fromJson (interop/write-json metadata))))

(defn with-metadata
  "Returns a new Dataset with `metadata` on the column `col-name`, in place of
  the metadata it had. `metadata` is a map of strings, numbers, booleans,
  vectors of one of those, and maps of the same, or Spark's `Metadata`.

  ```clojure
  (-> dataframe
      (g/with-metadata :price {:comment \"In AUD\"})
      (g/column-metadata :price))
  => {:comment \"In AUD\"}
  ```"
  [dataframe col-name metadata]
  (.withMetadata dataframe (name col-name) (->metadata metadata)))

(defn column-metadata
  "Returns the metadata of the top-level column `col-name`, as a map with
  keyword keys, or an empty map."
  [dataframe col-name]
  (-> dataframe
      .schema
      (.apply ^String (name col-name))
      .metadata
      .json
      interop/read-json))

(defn metadata-column
  "Returns a metadata column by its name, such as the `_metadata` column that
  file sources have, with each row's file path, name, size and modification
  time.

  ```clojure
  (let [dataframe (g/read-parquet! \"data.parquet\")]
    (g/select dataframe {:file (g/get-field (g/metadata-column dataframe \"_metadata\")
                                            :file_name)}))
  ```"
  [dataframe col-name]
  (.metadataColumn dataframe (name col-name)))

(defn observation
  "Creates a Spark `Observation`, with a name or a random one, for `observe`
  to fill and `observed` to read. Each one goes with a single `observe`."
  ([] (Observation.))
  ([observation-name] (Observation. ^String (name observation-name))))

(defn observe
  "Returns a new Dataset that computes the aggregates in `metrics` as an
  action runs on it, without changing its rows. `metrics` is a map of names to
  aggregate columns, or a seq of named aggregate columns. Given an
  `observation`, the metrics of the first action go to `observed`. Given a
  name instead, Spark only reports them to its query execution listeners.

  ```clojure
  (let [quality (g/observation)
        cleaned (g/observe dataframe quality {:rows   (g/count \"*\")
                                              :lowest (g/min :price)})]
    (g/write-parquet! cleaned \"cleaned.parquet\")
    (g/observed quality))
  => {:rows 13580, :lowest 85000.0}
  ```"
  [dataframe observation-or-name metrics]
  (let [[head & tail] (->col-array [metrics])
        target        (if (instance? Observation observation-or-name)
                        observation-or-name
                        (name observation-or-name))]
    (.observe dataframe target head (into-array Column tail))))

(defn observed
  "Returns the metrics that `observe` computed for the `observation`, as a map
  with keyword keys. It waits for the first action on the observed Dataset to
  finish, so call it after that action, or from another thread."
  [^Observation observation]
  (into {}
        (map (fn [[k v]] [(keyword k) (interop/->clojure v)]))
        (.getAsJava observation)))

(defn- explain-mode ^String [mode]
  (cond
    (true? mode)  "extended"
    (false? mode) "simple"
    :else         (name mode)))

(defn explain-string
  "Returns the plan that `explain` prints, as a string. `mode` is one of
  `:simple`, the default, `:extended`, `:codegen`, `:cost` and `:formatted`.

  ```clojure
  (g/explain-string dataframe :formatted)
  ```"
  ([dataframe] (explain-string dataframe :simple))
  ([dataframe mode]
   (let [mode (explain-mode mode)]
     (string/trimr (interop/with-scala-out-str (.explain dataframe mode))))))

(defn same-semantics
  "Returns true when the two Datasets' plans compute the same thing, as Spark
  sees it once it has analysed them. It doesn't run them."
  [dataframe other]
  (.sameSemantics dataframe other))

(defn semantic-hash
  "Returns a hash of the Dataset's analysed plan, which is equal for two
  Datasets that `same-semantics` finds the same."
  [dataframe]
  (.semanticHash dataframe))

;;;; Ungrouped
(defn spark-session [dataframe] (.sparkSession dataframe))

(defn sql-context [dataframe] (.sqlContext (.sparkSession dataframe)))

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
(import-fn unpivot melt)
(import-fn same-semantics same-semantics?)

