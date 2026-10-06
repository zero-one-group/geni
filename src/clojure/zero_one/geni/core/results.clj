(ns zero-one.geni.core.results
  "A DataFrame's result as Arrow IPC streams, tech.ml.dataset datasets and
  dtype-next tensors, all at once or a batch at a time, and two previews:
  glimpse and to-html. Arrow, tech.ml.dataset and dtype-next are resolved
  when they're needed, so this loads with only Spark's Connect client, and
  without tech.ml.dataset."
  (:require
   [clojure.string :as string]
   [zero-one.geni.core.column :refer [->col-array]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang IReduce Reflector)
   (java.util Iterator NoSuchElementException)
   (org.apache.spark.sql Row)
   (org.apache.spark.sql.types ArrayType CalendarIntervalType DataType MapType StructField
                               StructType UserDefinedType)))

;; Where the Arrow batches come from

(defn- classic? [df]
  (spark/classic-session? (.sparkSession df)))

(defn- reader
  "A function from zero-one.geni.arrow.reader, which needs Apache Arrow."
  [fn-name]
  (when-not (class-named "org.apache.arrow.vector.VectorSchemaRoot")
    (throw (ex-info (str "Decoding Arrow batches needs Apache Arrow, which classic Spark brings. "
                         "Add org.apache.arrow/arrow-vector and arrow-memory-netty to use it with "
                         "a Spark Connect client.")
                    {})))
  (requiring-resolve (symbol "zero-one.geni.arrow.reader" fn-name)))

(defn- concat-bytes ^bytes [chunks]
  (if (= 1 (count chunks))
    (first chunks)
    (let [out (java.io.ByteArrayOutputStream.)]
      (doseq [^bytes chunk chunks] (.write out chunk))
      (.toByteArray out))))

(defn- chunks-in-batch
  "How many responses an Arrow batch from Spark Connect comes in. Spark 4.1's
  protocol added chunking, so 4.0's batches have no such field."
  [batch]
  (try
    (when (.hasNumChunksInBatch batch) (.getNumChunksInBatch batch))
    (catch IllegalArgumentException _ nil)))

(defn- next-connect-stream
  "The next Arrow IPC stream in a Spark Connect execution's responses, with
  its chunks put together, or nil at the end."
  [responses]
  (loop [chunks []]
    (when (.hasNext responses)
      (let [response (.next responses)]
        (if-not (.hasArrowBatch response)
          (recur chunks)
          (let [batch  (.getArrowBatch response)
                chunks (conj chunks (.toByteArray (.getData batch)))
                total  (or (chunks-in-batch batch) 0)]
            (if (<= total (count chunks))
              (concat-bytes chunks)
              (recur chunks))))))))

(defn- connect-streams
  "The result of `df` over Spark Connect as the Arrow IPC streams that the
  server sends, one per batch, read as they arrive: what Latu reads. It goes
  through the client's `executeInternal`, which is private to Spark's sql
  package, from Spark 4.0 to 4.2."
  [df]
  (let [responses (Reflector/invokeInstanceMethod (.sparkSession df) "executeInternal"
                                                  (object-array [(.plan df)]))
        pending   (volatile! ::unread)
        advance!  #(when (identical? ::unread @pending)
                     (vreset! pending (next-connect-stream responses)))]
    {:iterator (reify Iterator
                 (hasNext [_] (advance!) (some? @pending))
                 (next [_]
                   (advance!)
                   (if-let [stream @pending]
                     (do (vreset! pending ::unread) stream)
                     (throw (NoSuchElementException.)))))
     :close    #(.close responses)}))

(defn- open-streams
  "Starts reading the result of `df` as Arrow IPC streams, one per batch.
  With `:all`, classic Spark collects every batch in one job; with `:lazy`,
  it runs a job per partition as the iterator is read. Spark Connect sends
  the batches as the server makes them. Returns the iterator, and a function
  that stops the reading."
  [df mode]
  (if (classic? df)
    {:iterator ((reader "classic-streams") df mode) :close (fn [])}
    (connect-streams df)))

(defn- as-sql-execution
  "Calls `f`, which reads the result of `df`, as one of Spark's SQL
  executions, named `fn-name`, as `collect` reads it, so that the query's
  listeners see it end: an Observation from `observe` gets its metrics, and
  Spark's UI lists the query. A Spark Connect server does that itself."
  [df fn-name f]
  (if (classic? df)
    ;; SQLExecution is in Spark's execution package, which isn't its API.
    (Reflector/invokeInstanceMethod
     (Reflector/getStaticField "org.apache.spark.sql.execution.SQLExecution$" "MODULE$")
     "withNewExecutionId"
     (object-array [(.queryExecution df) (scala.Some. fn-name) (interop/->scala-function0 f)]))
    (f)))

(defn- all-streams [df fn-name]
  (as-sql-execution
   df fn-name
   #(let [{:keys [^Iterator iterator close]} (open-streams df :all)]
      (try
        (loop [out []]
          (if (.hasNext iterator)
            (recur (conj out (.next iterator)))
            out))
        (finally (close))))))

;; The types that a dataset can't hold

(def ^:private refused-types
  "Spark 4.1's spatial types, which Spark 3.5 and 4.0 don't have."
  {"org.apache.spark.sql.types.GeometryType"  "geometries"
   "org.apache.spark.sql.types.GeographyType" "geographies"})

(defn- refused
  "What a value of Spark type `dt` holds when tech.ml.dataset has nothing for
  it, inside arrays, maps and structs too, or nil."
  [^DataType dt]
  (cond
    (instance? CalendarIntervalType dt) "calendar intervals"
    (instance? ArrayType dt)            (refused (.elementType ^ArrayType dt))
    (instance? MapType dt)              (or (refused (.keyType ^MapType dt))
                                            (refused (.valueType ^MapType dt)))
    (instance? StructType dt)           (some #(refused (.dataType ^StructField %))
                                              (.fields ^StructType dt))
    (instance? UserDefinedType dt)      (when-not (= "org.apache.spark.ml.linalg.VectorUDT"
                                                     (.getName (class dt)))
                                          (refused (.sqlType ^UserDefinedType dt)))
    :else                               (some (fn [[class-name what]]
                                                (when (some-> (class-named class-name)
                                                              (.isInstance dt))
                                                  what))
                                              refused-types)))

(defn- check-names!
  "Throws when the result has two columns of one name."
  [fn-name ^StructType schema]
  (let [names (map #(.name ^StructField %) (.fields schema))]
    (when-let [twice (some (fn [[n k]] (when (< 1 k) n)) (frequencies names))]
      (throw (ex-info (str fn-name " needs distinct column names, but two columns are named \""
                           twice "\". Rename one first.")
                      {:column twice})))))

(defn- output-names
  "The names that `key-fn` gives the result's columns, checked before a job
  runs: two columns of one name throw, and so do two columns that `key-fn`
  gives one name, which would otherwise lose one of them."
  [fn-name ^StructType schema key-fn]
  (check-names! fn-name schema)
  (let [names  (mapv #(.name ^StructField %) (.fields schema))
        output (mapv key-fn names)]
    (when-let [[k sources] (some (fn [[k pairs]]
                                   (when (< 1 (count pairs)) [k (mapv second pairs)]))
                                 (group-by first (map vector output names)))]
      (throw (ex-info (str fn-name "'s :key-fn gives the columns "
                           (string/join " and " (map #(str "\"" % "\"") sources))
                           " one name, " (pr-str k) ". Pass a :key-fn that keeps them apart, "
                           "or rename one of the columns first.")
                      {:key k :columns sources})))
    output))

(defn- check-columns!
  "Throws for a batch with rows but no columns, since `what`, a dataset or a
  map of tensors, holds rows only in its columns."
  [fn-name what row-count n-columns]
  (when (and (zero? n-columns) (pos? row-count))
    (throw (ex-info (str fn-name " can't give rows without columns: the result has rows but no "
                         "columns, and " what " holds rows only in its columns. Select at least "
                         "one column first.")
                    {:rows row-count}))))

(defn- twice-named-field
  "A name that two fields of one struct have, inside a value of Spark type
  `dt`, or nil. A map's struct fields are named by Spark's names, so the
  second would be lost."
  [^DataType dt]
  (cond
    (instance? ArrayType dt)  (twice-named-field (.elementType ^ArrayType dt))
    (instance? MapType dt)    (or (twice-named-field (.keyType ^MapType dt))
                                  (twice-named-field (.valueType ^MapType dt)))
    (instance? StructType dt) (let [fields (.fields ^StructType dt)]
                                (or (some (fn [[n k]] (when (< 1 k) n))
                                          (frequencies (map #(.name ^StructField %) fields)))
                                    (some #(twice-named-field (.dataType ^StructField %)) fields)))
    :else                     nil))

(defn- check-types!
  "Throws when the result has a column of a type that has no tech.ml.dataset
  equivalent, or a struct with two fields of one name."
  [fn-name ^StructType schema]
  (doseq [^StructField field (.fields schema)]
    (when-let [what (refused (.dataType field))]
      (throw (ex-info (str fn-name " can't convert the column \"" (.name field) "\", which holds "
                           what ". Cast it to another type first, such as a string.")
                      {:column (.name field)})))
    (when-let [twice (twice-named-field (.dataType field))]
      (throw (ex-info (str fn-name " can't convert the column \"" (.name field) "\", which has "
                           "a struct with two fields named \"" twice "\". Rename one first, "
                           "such as with g/struct.")
                      {:column (.name field) :field twice})))))

;; tech.ml.dataset and dtype-next

(defn- tmd
  "A function from tech.ml.dataset or dtype-next, which it brings. Called
  before a job starts too, so that a missing library fails fast."
  [sym]
  (or (try (requiring-resolve sym) (catch Exception _ nil))
      (throw (ex-info (str "This needs tech.ml.dataset, which isn't on the classpath. Add "
                           "techascent/tech.ml.dataset to your dependencies.")
                      {}))))

(defn- typed-array-concat
  "The arrays, which have one component type, as one."
  [arrays]
  (if (= 1 (count arrays))
    (first arrays)
    (let [total (reduce + (map #(java.lang.reflect.Array/getLength %) arrays))
          out   (java.lang.reflect.Array/newInstance (.getComponentType (class (first arrays)))
                                                     (int total))]
      (reduce (fn [offset a]
                (let [n (java.lang.reflect.Array/getLength a)]
                  (System/arraycopy a 0 out offset n)
                  (+ offset n)))
              0
              arrays)
      out)))

(defn- concat-batches
  "Decoded batches as one, column by column."
  [batches]
  (if (= 1 (count batches))
    (first batches)
    (let [offsets (reductions + 0 (map :row-count batches))]
      {:row-count (last offsets)
       :columns   (apply mapv
                         (fn [& columns]
                           (let [missing (mapcat (fn [column offset]
                                                   (map #(+ offset %) (:missing column)))
                                                 columns
                                                 offsets)]
                             (assoc (first columns)
                                    :data (typed-array-concat (map :data columns))
                                    :missing (when (seq missing) (int-array missing)))))
                         (map :columns batches))})))

(defn- user-defined?
  "Whether a value of Spark type `dt` holds a user-defined type's, such as an
  MLlib vector, at the top or inside."
  [^DataType dt]
  (cond
    (instance? UserDefinedType dt) true
    (instance? ArrayType dt)       (user-defined? (.elementType ^ArrayType dt))
    (instance? MapType dt)         (or (user-defined? (.keyType ^MapType dt))
                                       (user-defined? (.valueType ^MapType dt)))
    (instance? StructType dt)      (boolean (some #(user-defined? (.dataType ^StructField %))
                                                  (.fields ^StructType dt)))
    :else                          false))

(defn- tmd-columns
  "Each of the result's columns as a dataset gets it: its name, as `key-fn`
  gives it, and its Spark type as DDL in its metadata, under
  `:zero-one.geni/spark-type`, for create-dataframe, except for one that
  holds MLlib vectors, at the top or inside, whose DDL would be their
  storage's."
  [fn-name ^StructType schema key-fn]
  (mapv (fn [k ^StructField field]
          {:name     k
           :metadata (when-not (user-defined? (.dataType field))
                       {:zero-one.geni/spark-type (.sql (.dataType field))})})
        (output-names fn-name schema key-fn)
        (.fields schema)))

(defn- ->tmd-dataset
  "A tech.ml.dataset dataset of a decoded batch, with `columns`, from
  `tmd-columns`, giving each column's name and metadata. Each column keeps
  its datatype, even with no values, or none that aren't missing, which
  tech.ml.dataset would otherwise take for booleans."
  [{:keys [row-count] decoded :columns} fn-name columns]
  (check-columns! fn-name "a dataset" row-count (count decoded))
  (let [new-dataset  (tmd 'tech.v3.dataset/new-dataset)
        array-buffer (tmd 'tech.v3.datatype.array-buffer/array-buffer)
        ->bitmap     (tmd 'tech.v3.datatype.bitmap/->bitmap)]
    (new-dataset
     (mapv (fn [{:keys [datatype data missing]} {:keys [name metadata]}]
             {:tech.v3.dataset/name            name
              :tech.v3.dataset/data            (array-buffer data datatype)
              :tech.v3.dataset/missing         (->bitmap (or missing []))
              :tech.v3.dataset/metadata        metadata
              :tech.v3.dataset/force-datatype? true})
           decoded
           columns))))

(defn- ->tensors
  "dtype-next tensors of decoded tensor data, by column name."
  [{:keys [row-count columns]} fn-name col-names]
  (check-columns! fn-name "a map of tensors" row-count (count columns))
  (let [reshape (tmd 'tech.v3.tensor/reshape)]
    (zipmap col-names
            (map (fn [{:keys [data shape]}] (reshape data shape)) columns))))

;; Reducing over batches

(defn- preserving-reduced
  "A reducing function that wraps `f`'s reduced results once more, so that an
  inner reduce hands a reduced value on to an outer one."
  [f]
  (fn [acc item]
    (let [result (f acc item)]
      (if (reduced? result) (reduced result) result))))

(deftype Batches [df fn-name mode decode open-runs]
  ;; A reduce reads the batches as one of Spark's SQL executions.
  IReduce
  (reduce [_ f init]
    (as-sql-execution
     df fn-name
     #(let [{:keys [^Iterator iterator close]} (open-streams df mode)
            f (preserving-reduced f)]
        (try
          (loop [acc init]
            (if (.hasNext iterator)
              (let [acc (reduce f acc (decode (.next iterator)))]
                (if (reduced? acc) @acc (recur acc)))
              acc))
          (finally (close))))))

  ;; Without an init, as `reduce` does a collection: `f` of the first two
  ;; items, the first item when it's the only one, and `(f)` when there are
  ;; none.
  (reduce [this f]
    (let [result (.reduce this
                          (fn [acc item] (if (identical? acc ::none) item (f acc item)))
                          ::none)]
      (if (identical? result ::none) (f) result)))

  ;; A seq reads a batch at a time, where Clojure's seq of an Iterable would
  ;; read 32 ahead.
  clojure.lang.Seqable
  (seq [this]
    (let [^Iterator iterator (.iterator ^Iterable this)
          step               (fn step []
                               (lazy-seq
                                (when (.hasNext iterator)
                                  (cons (.next iterator) (step)))))]
      (seq (step))))

  Iterable
  (iterator [_]
    (let [{:keys [^Iterator iterator close]} (open-streams df mode)
          _      (swap! open-runs conj close)
          items  (volatile! nil)
          done!  (fn [] (close) (swap! open-runs disj close))
          ready? (fn []
                   (loop []
                     (cond
                       (seq @items)       true
                       (.hasNext iterator) (do (vreset! items (seq (decode (.next iterator))))
                                               (recur))
                       :else              (do (done!) false))))]
      (reify Iterator
        (hasNext [_] (ready?))
        (next [_]
          (if (ready?)
            (let [[item & more] @items] (vreset! items more) item)
            (throw (NoSuchElementException.)))))))

  java.lang.AutoCloseable
  (close [_]
    (doseq [close @open-runs] (close))
    (reset! open-runs #{})))

(defn- batches [df fn-name mode decode]
  (Batches. df fn-name mode decode (atom #{})))

;; The public functions

(defn to-arrow
  "The result of `dataframe` as Apache Arrow IPC streams in memory, one per
  batch: a vector of byte arrays, each a complete stream of the schema, one
  record batch and the end marker, for any Arrow reader. They must never be
  concatenated byte for byte. An empty result gives one stream with no rows.

  On classic Spark, the batches are Spark's own, as PySpark's `toPandas`
  gets them, of at most `spark.sql.execution.arrow.maxRecordsPerBatch` rows
  (10,000 by default). Over Spark Connect, they're the ones the server sends.
  Like `collect`, the whole result comes to the driver."
  [dataframe]
  (all-streams dataframe "to-arrow"))

(defn to-tmd
  "The result of `dataframe` as one tech.ml.dataset dataset, which needs
  `techascent/tech.ml.dataset` on the classpath. Like `collect`, the whole
  result comes to the driver; `stream` is for one batch at a time. An empty
  result gives a dataset with no rows and the result's columns.

  Columns are named by keyword, or by `:key-fn` applied to the name. A null
  is a missing value. Numbers and booleans stay primitive. DECIMAL becomes a
  BigDecimal, DATE a LocalDate, TIMESTAMP an Instant, TIMESTAMP_NTZ a
  LocalDateTime, TIME a LocalTime, a day-time interval a Duration, a
  year-month interval a Period and BINARY a byte array. An array becomes a
  vector, and a struct or a map a map, with keyword keys for a struct's
  fields. VARIANT becomes Spark's VariantVal, and an MLlib vector what
  `collect` gives. Each column keeps its Spark type, as DDL, in its metadata
  under `:zero-one.geni/spark-type`, which `create-dataframe` uses, but for
  one that holds MLlib vectors.

  A calendar interval, a geometry or a geography throws, as do a struct with
  two fields of one name, two columns of one name, and two that `:key-fn`
  names alike, before a job runs. So do rows without columns, which a
  dataset can't hold. On classic Spark, it runs as one of Spark's SQL
  executions, as `collect` does, so an Observation from `observe` gets its
  metrics.

  Over Spark Connect, it needs `org.apache.arrow/arrow-vector` and
  `arrow-memory-netty` on the classpath, since the client's Arrow is shaded."
  ([dataframe] (to-tmd dataframe {}))
  ([dataframe {:keys [key-fn] :or {key-fn keyword}}]
   (let [schema  (.schema dataframe)
         _       (check-types! "to-tmd" schema)
         columns (tmd-columns "to-tmd" schema key-fn)
         decode  (reader "decode")
         _       (tmd 'tech.v3.dataset/new-dataset)]
     (-> (mapcat #(decode % schema) (all-streams dataframe "to-tmd"))
         concat-batches
         (->tmd-dataset "to-tmd" columns)))))

(defn stream
  "The result of `dataframe` as tech.ml.dataset datasets, one per Arrow batch
  that has rows, each as `to-tmd` makes it, with its options.

  Returns a reducible, so that `reduce`, `transduce`, `into` and `run!` read
  the batches as they go, and stop reading when they're done, stop early or
  throw. It's also seqable, for `seq`, `first` and `doseq`, which read a
  batch at a time but can stop before the end, so close it with `with-open`
  for those:

  ```clojure
  (transduce (map tech.v3.dataset/row-count) + (g/stream df))
  (with-open [batches (g/stream df)]
    (first batches))
  ```

  On classic Spark, each partition runs as a job of its own, as a reduce
  gets to it, so only one partition's batches are on the driver at a time,
  and a reduce is one of Spark's SQL executions, as `collect` is, so an
  Observation from `observe` gets its metrics at its end, which a seq
  doesn't give it. Over Spark Connect, the server sends the batches as it
  makes them, and stopping releases the execution."
  ([dataframe] (stream dataframe {}))
  ([dataframe {:keys [key-fn] :or {key-fn keyword}}]
   (let [schema  (.schema dataframe)
         _       (check-types! "stream" schema)
         columns (tmd-columns "stream" schema key-fn)
         decode  (reader "decode")
         _       (tmd 'tech.v3.dataset/new-dataset)]
     (batches dataframe "stream" :lazy
              (fn [ipc]
                (->> (decode ipc schema)
                     (filter #(pos? (:row-count %)))
                     (map #(->tmd-dataset % "stream" columns))))))))

(defn- tensor-columns
  "The result's column names and indices for to-tensors and stream-tensors,
  which select `columns` first when they're given."
  [fn-name dataframe columns key-fn]
  (let [dataframe (if (seq columns) (.select dataframe (->col-array columns)) dataframe)
        schema    (.schema dataframe)]
    {:dataframe dataframe
     :schema    schema
     :names     (output-names fn-name schema key-fn)
     :indices   (vec (range (count (.fields schema))))}))

(defn to-tensors
  "The result of `dataframe` as dtype-next tensors, one per column, in a map
  by column name: by keyword, or by `:key-fn` applied to the name.
  `:columns` selects the columns first. The whole result comes to the
  driver; `stream-tensors` is for one batch at a time.

  A column of integers (TINYINT, SMALLINT, INT or BIGINT) or floating-point
  numbers (FLOAT or DOUBLE) with no nulls becomes a tensor of shape [rows],
  and a column of arrays of those, or of dense MLlib vectors, all of one
  length, a tensor of shape [rows length]. Anything else throws, naming the
  column: nulls, DECIMALs, strings, booleans, arrays of different lengths
  and sparse vectors. So do an empty result, rows without columns, and two
  columns that `:key-fn` names alike.

  ```clojure
  (g/to-tensors scored {:columns [:features :label]})
  ;; => {:features #tech.v3.tensor<float64>[1000 4] ..., :label ...}
  ```

  It needs dtype-next on the classpath, which tech.ml.dataset brings, and
  over Spark Connect, `org.apache.arrow/arrow-vector` and
  `arrow-memory-netty`."
  ([dataframe] (to-tensors dataframe {}))
  ([dataframe {:keys [columns key-fn] :or {key-fn keyword}}]
   (let [{:keys [dataframe schema names indices]} (tensor-columns "to-tensors" dataframe columns key-fn)
         decode  (reader "decode-tensors")
         _       (tmd 'tech.v3.tensor/reshape)
         decoded (->> (all-streams dataframe "to-tensors")
                      (mapcat #(decode % schema indices "to-tensors"))
                      (filter #(pos? (:row-count %))))]
     (when (empty? decoded)
       (throw (ex-info "to-tensors needs at least one row, and the result is empty." {})))
     (->tensors
      {:row-count (reduce + (map :row-count decoded))
       :columns   (apply mapv
                         (fn [^StructField field & columns]
                           ;; Each batch checks its own rows, so this checks
                           ;; the batches against each other.
                           (let [widths (set (map #(vec (rest (:shape %))) columns))]
                             (when (< 1 (count widths))
                               ((reader "refuse!") (.name field) ", whose arrays aren't all one length"))
                             {:data  (typed-array-concat (map :data columns))
                              :shape (into [(reduce + (map #(first (:shape %)) columns))]
                                           (first widths))}))
                         (.fields ^StructType schema)
                         (map :columns decoded))}
      "to-tensors"
      names))))

(defn stream-tensors
  "The result of `dataframe` as maps of dtype-next tensors, one map per Arrow
  batch that has rows, each as `to-tensors` makes it, with its options. Each
  batch's tensors are its own: stacking them is up to the caller.

  Returns a reducible, as `stream` does, which stops reading when a reduce is
  done, stops early or throws. It's seqable too, a batch at a time, so close
  it with `with-open` when reading it as a seq."
  ([dataframe] (stream-tensors dataframe {}))
  ([dataframe {:keys [columns key-fn] :or {key-fn keyword}}]
   (let [{:keys [dataframe schema names indices]} (tensor-columns "stream-tensors" dataframe columns key-fn)
         decode (reader "decode-tensors")
         _      (tmd 'tech.v3.tensor/reshape)]
     (batches dataframe "stream-tensors" :lazy
              (fn [ipc]
                (->> (decode ipc schema indices "stream-tensors")
                     (filter #(pos? (:row-count %)))
                     (map #(->tensors % "stream-tensors" names))))))))

(defn- glimpse-value
  "A value as glimpse shows it: Clojure's printed form for strings, numbers,
  booleans, keywords and nil, collections with their contents shown the same
  way, and a date, a time or another object as its string."
  [value]
  (cond
    (or (nil? value) (string? value) (number? value) (boolean? value) (keyword? value))
    (pr-str value)

    (map? value)
    (str "{" (string/join ", " (map (fn [[k v]] (str (glimpse-value k) " " (glimpse-value v)))
                                    value))
         "}")

    (coll? value)
    (str "[" (string/join " " (map glimpse-value value)) "]")

    :else
    (str value)))

(defn- glimpse-line [^String line width]
  (if (> (count line) width)
    (str (subs line 0 (dec width)) "…")
    line))

(defn glimpse
  "Prints a transposed preview of `dataframe`: one line per column, with its
  name, its type and its first few values, which reads better than `show`
  on a wide DataFrame.

  ```clojure
  (g/glimpse df)
  ; Rows: at least 10
  ; Columns: 3
  ; $ id    <bigint> 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
  ; $ name  <string> \"Ada\", \"Bo\", nil, \"Grace\", nil, \"Alan\", …
  ; $ score <double> 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, …
  ```

  Strings, numbers and nulls print as Clojure data, so a string is quoted and
  a null is nil, and dates, times and other objects as their strings.
  `Rows:` is exact only when the sample comes back short; otherwise it says
  \"at least\", since counting the rows takes a full pass over the data.

  Options:
  - `:num-rows`, the values to show per column, 10 by default;
  - `:width`, where to cut a line, 80 by default, or `##Inf` to cut nothing;
  - `:count`, true to count the rows for an exact `Rows:`."
  ([dataframe] (glimpse dataframe {}))
  ([dataframe {:keys [num-rows width] count? :count :or {num-rows 10 width 80}}]
   (when-not (pos-int? num-rows)
     (throw (IllegalArgumentException.
             (str ":num-rows is a positive integer, not " (pr-str num-rows) ". For the "
                  "columns and their types alone, g/dtypes and g/print-schema are cheaper."))))
   (when-not (or (pos-int? width) (= ##Inf width))
     (throw (IllegalArgumentException.
             (str ":width is a positive integer or ##Inf, not " (pr-str width) "."))))
   (let [fields (.fields ^StructType (.schema dataframe))
         rows   (.collectAsList (.limit dataframe (int num-rows)))
         taken  (.size rows)
         label  (cond count?            (str (.count dataframe))
                      (< taken num-rows) (str taken)
                      :else             (str "at least " num-rows))
         pad    (reduce max 0 (map #(count (.name ^StructField %)) fields))
         line   (fn [i ^StructField field]
                  (let [values (map #(glimpse-value (interop/->clojure (.get ^Row % (int i)))) rows)]
                    (glimpse-line
                     (.stripTrailing
                      (format (str "$ %-" (max 1 pad) "s <%s> %s")
                              (.name field)
                              (.simpleString (.dataType field))
                              (apply str (interpose ", " values))))
                     width)))]
     (print (apply str
                   "Rows: " label "\n"
                   "Columns: " (count fields) "\n"
                   (map-indexed #(str (line %1 %2) "\n") fields)))
     (flush))))

(defn- html-over-connect
  "Spark Connect's `HtmlString` relation, which the server renders, as
  PySpark's `_repr_html_` does over Spark Connect."
  [dataframe num-rows truncate]
  (let [root (.getRoot (.plan dataframe))
        html (.newDataFrame (.sparkSession dataframe)
                            (interop/->scala-function1
                             (fn [builder]
                               (-> (.getHtmlStringBuilder builder)
                                   (.setInput root)
                                   (.setNumRows (int num-rows))
                                   (.setTruncate (int truncate)))
                               nil)))]
    (.getString ^Row (first (.collectAsList html)) 0)))

(defn to-html
  "The first rows of `dataframe` as an HTML table, as Spark renders a
  DataFrame in a notebook: PySpark's `_repr_html_`. Spark escapes the cells,
  and a note under the table says when there are more rows than it shows.

  Options:
  - `:num-rows`, the rows to show, 20 by default;
  - `:truncate`, the width that a cell is cut to, 20 by default, or 0 or
    false not to cut."
  ([dataframe] (to-html dataframe {}))
  ([dataframe {:keys [num-rows truncate] :or {num-rows 20 truncate 20}}]
   (let [truncate (cond (true? truncate) 20 (false? truncate) 0 :else truncate)]
     (when-not (and (nat-int? num-rows) (nat-int? truncate))
       (throw (IllegalArgumentException.
               (str ":num-rows and :truncate take a non-negative integer, not "
                    (pr-str (if (nat-int? num-rows) truncate num-rows)) "."))))
     (if (classic? dataframe)
       ;; Dataset.htmlString, which is private to Spark's sql package.
       (Reflector/invokeInstanceMethod dataframe "htmlString"
                                       (object-array [(int num-rows) (int truncate)]))
       (html-over-connect dataframe num-rows truncate)))))
