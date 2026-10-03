(ns zero-one.geni.core.data-sources
  (:require
   [clojure.edn :as edn]
   [clojure.string :as string]
   [clojure.java.io :as io]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.core.column :as column]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.core.dataset :as dataset]
   [zero-one.geni.core.function-table :as function-table]
   [zero-one.geni.utils :refer [->camel-case ->kebab-case ensure-coll]])
  (:import
   (java.text Normalizer Normalizer$Form)
   (org.apache.spark.sql Column Dataset DataFrameWriter Encoders SparkSession)))

(defn- configure-reader-or-writer
  "Sets each option: a keyword key in camelCase, such as `:infer-schema` as
  `inferSchema`, and a string key as it is, for options such as Iceberg's
  `\"snapshot-id\"`. A keyword value goes as its name."
  [unconfigured options]
  (reduce
   (fn [r [k v]]
     (.option r (if (string? k) k (->camel-case k)) (if (keyword? v) (name v) v)))
   unconfigured
   options))

(def default-options
  "Default DataFrameReader options."
  {"csv" {:header "true" :infer-schema "true"}})

(defn- deaccent [string]
  ;; Source: https://gist.github.com/maio/e5f85d69c3f6ca281ccd
  (let [normalized (Normalizer/normalize string Normalizer$Form/NFD)]
    (string/replace normalized #"\p{InCombiningDiacriticalMarks}+" "")))

(defn- remove-punctuations [string]
  (string/replace string #"[.,\/#!$%\^&\*;:{}=\`~()°]" ""))

(defn ->kebab-columns
  "Returns a new Dataset with all columns renamed to kebab cases."
  [dataset]
  (let [new-columns (->> dataset
                         .columns
                         (map remove-punctuations)
                         (map deaccent)
                         (map ->kebab-case))]
    (.toDF dataset (interop/->scala-seq new-columns))))

(defn- read-data! [format-name spark path options]
  (let [reader-opts (dissoc options :kebab-columns :schema)
        defaults    (default-options format-name)
        schema      (:schema options)
        reader      (-> (.. spark read (format format-name))
                        (configure-reader-or-writer (merge defaults reader-opts))
                        (cond-> (not (nil? schema))
                          (.schema (dataset-creation/->schema schema))))]
    (-> (.load reader path)
        (cond-> (:kebab-columns options) ->kebab-columns))))

(defmulti read-avro!
  "Loads an Avro file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  (fn [head & _] (class head)))
(defmethod read-avro! :default
  ([path] (read-avro! @defaults/spark path))
  ([path options] (read-avro! @defaults/spark path options)))
(defmethod read-avro! SparkSession
  ([spark path] (read-avro! spark path {}))
  ([spark path options] (read-data! "avro" spark path options)))

(defmulti read-parquet!
  "Loads a Parquet file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html"
  (fn [head & _] (class head)))
(defmethod read-parquet! :default
  ([path] (read-parquet! @defaults/spark path))
  ([path options] (read-parquet! @defaults/spark path options)))
(defmethod read-parquet! SparkSession
  ([spark path] (read-parquet! spark path {}))
  ([spark path options] (read-data! "parquet" spark path options)))

(defmulti read-binary!
  "Loads a binary file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html"
  (fn [head & _] (class head)))
(defmethod read-binary! :default
  ([path] (read-binary! @defaults/spark path))
  ([path options] (read-binary! @defaults/spark path options)))
(defmethod read-binary! SparkSession
  ([spark path] (read-binary! spark path {}))
  ([spark path options] (read-data! "binaryFile" spark path options)))

(defmulti read-csv!
  "Loads a CSV file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  (fn [head & _] (class head)))
(defmethod read-csv! :default
  ([path] (read-csv! @defaults/spark path))
  ([path options] (read-csv! @defaults/spark path options)))
(defmethod read-csv! SparkSession
  ([spark path] (read-csv! spark path {}))
  ([spark path options] (read-data! "csv" spark path options)))

(defmulti read-libsvm!
  "Loads a LIBSVM file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  (fn [head & _] (class head)))
(defmethod read-libsvm! :default
  ([path] (read-libsvm! @defaults/spark path))
  ([path options] (read-libsvm! @defaults/spark path options)))
(defmethod read-libsvm! SparkSession
  ([spark path] (read-libsvm! spark path {}))
  ([spark path options] (read-data! "libsvm" spark path options)))

(defmulti read-json!
  "Loads a JSON file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  (fn [head & _] (class head)))
(defmethod read-json! :default
  ([path] (read-json! @defaults/spark path))
  ([path options] (read-json! @defaults/spark path options)))
(defmethod read-json! SparkSession
  ([spark path] (read-json! spark path {}))
  ([spark path options] (read-data! "json" spark path options)))

(defmulti read-text!
  "Loads a text file and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  (fn [head & _] (class head)))
(defmethod read-text! :default
  ([path] (read-text! @defaults/spark path))
  ([path options] (read-text! @defaults/spark path options)))
(defmethod read-text! SparkSession
  ([spark path] (read-text! spark path {}))
  ([spark path options] (read-data! "text" spark path options)))

(defn read-jdbc!
  "Loads a database table and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  ([options] (read-jdbc! @defaults/spark options))
  ([spark options]
   (let [unconfigured-reader (.. spark sqlContext read (format "jdbc"))
         configured-reader   (configure-reader-or-writer unconfigured-reader
                                                         (dissoc options :kebab-columns))]
     (cond-> (.load configured-reader)
       (:kebab-columns options) ->kebab-columns))))

(defn- partition-by-arg [partition-id]
  (into-array java.lang.String (map name (ensure-coll partition-id))))

(defn- mode-name
  "A writer's :mode, which can be a keyword, as Spark takes it."
  [mode]
  (if (keyword? mode) (name mode) mode))

(defn- ->names [cols]
  (map name (ensure-coll cols)))

(defn- bucket-writer [writer bucket-spec]
  (let [[n-buckets & cols]  (when (sequential? bucket-spec) bucket-spec)
        [col-name & others] (->names (mapcat ensure-coll cols))]
    (when-not (and (integer? n-buckets) col-name)
      (throw (ex-info (str ":bucket-by takes the number of buckets and the columns, such as "
                           "[8 :id]. Got: " (pr-str bucket-spec))
                      {:bucket-by bucket-spec})))
    (.bucketBy writer (int n-buckets) col-name (into-array String others))))

(defn- sort-writer [writer cols]
  (let [[col-name & others] (->names cols)]
    (.sortBy writer col-name (into-array String others))))

(defn- cluster-writer [writer cols]
  (let [[col-name & others] (->names cols)]
    (.clusterBy writer col-name (into-array String others))))

(defn- configure-base-writer ^DataFrameWriter
  [writer options]
  (let [{:keys [format mode partition-by bucket-by sort-by cluster-by]} options
        _      (when cluster-by (spark/require-version! [4 0] ":cluster-by"))
        writer (-> writer
                   (cond-> format (.format (name format)))
                   (cond-> mode (.mode (mode-name mode)))
                   (cond-> partition-by (.partitionBy (partition-by-arg partition-by)))
                   (cond-> bucket-by (bucket-writer bucket-by))
                   (cond-> sort-by (sort-writer sort-by))
                   (cond-> cluster-by (cluster-writer cluster-by)))]
    (configure-reader-or-writer
     writer
     (dissoc options :format :mode :partition-by :bucket-by :sort-by :cluster-by))))

(defn- write-data! [format dataframe path options]
  (let [configured-writer (-> (.write dataframe)
                              (.format format)
                              (configure-base-writer options))]
    (.save configured-writer path)))

(defn write-parquet!
  "Writes a Parquet file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html"
  ([dataframe path] (write-parquet! dataframe path {}))
  ([dataframe path options] (write-data! "parquet" dataframe path options)))

(defn write-csv!
  "Writes a CSV file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  ([dataframe path] (write-csv! dataframe path {"header" "true"}))
  ([dataframe path options] (write-data! "csv" dataframe path (merge options {"header" "true"}))))

(defn write-libsvm!
  "Writes a LIBSVM file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  ([dataframe path] (write-libsvm! dataframe path {}))
  ([dataframe path options] (write-data! "libsvm" dataframe path options)))

(defn write-json!
  "Writes a JSON file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources-json.html"
  ([dataframe path] (write-json! dataframe path {}))
  ([dataframe path options] (write-data! "json" dataframe path options)))

(defn write-text!
  "Writes a text file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  ([dataframe path] (write-text! dataframe path {}))
  ([dataframe path options] (write-data! "text" dataframe path options)))

(defn write-avro!
  "Writes an Avro file at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  ([dataframe path] (write-avro! dataframe path {}))
  ([dataframe path options] (write-data! "avro" dataframe path options)))

(defn write-jdbc!
  "Writes a database table.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html"
  [dataframe options]
  (let [mode                (:mode options)
        unconfigured-writer (-> dataframe
                                (.write)
                                (.format "jdbc")
                                (cond-> mode (.mode (mode-name mode))))
        configured-writer   (configure-reader-or-writer
                             unconfigured-writer
                             (dissoc options :mode))]
    (.save configured-writer)))

;; EDN
(defn- file-exists? [path]
  (.exists (io/file path)))

(defn- ensure-writable! [path options]
  (when (and (file-exists? path) (not= (mode-name (:mode options)) "overwrite"))
    (throw (Exception. (format "path file:%s already exists!" path)))))

(defn write-edn!
  "Writes an EDN file at the specified path."
  ([dataframe path] (write-edn! dataframe path {}))
  ([dataframe path options]
   (ensure-writable! path options)
   (spit path (->> dataframe .toJSON .collect (mapv interop/read-json)))))

(defmulti read-edn!
  "Loads an EDN file and returns the results as a DataFrame."
  (fn [head & _] (class head)))
(defmethod read-edn! :default
  ([path] (read-edn! @defaults/spark path))
  ([path options] (read-edn! @defaults/spark path options)))
(defmethod read-edn! SparkSession
  ([spark path] (read-edn! spark path {}))
  ([spark path options]
   (let [dataset (->> path
                      slurp
                      edn/read-string
                      (dataset-creation/records->dataset spark))]
     (-> dataset
         (cond-> (:kebab-columns options) ->kebab-columns)))))

;; Excel
(defn- fxl
  "Resolves a function from zero.one/fxl, which Excel support needs."
  [fn-name]
  (or (try
        (requiring-resolve (symbol "zero-one.fxl.core" fn-name))
        (catch Exception _ nil))
      (throw (ex-info (str "Excel support needs zero.one/fxl. Add it to your "
                           "dependencies to use read-xlsx! and write-xlsx!.")
                      {}))))

(defn write-xlsx!
  "Writes an Excel file at the specified path. Needs `zero.one/fxl` on the
  classpath."
  ([dataframe path] (write-xlsx! dataframe path {}))
  ([dataframe path options]
   (ensure-writable! path options)
   ((fxl "write-xlsx!")
    ((fxl "concat-below")
     ((fxl "row->cells") (dataset/column-names dataframe))
     ((fxl "records->cells") (dataset/columns dataframe) (dataset/collect dataframe)))
    path)))

(defmulti read-xlsx!
  "Loads an Excel file and returns the results as a DataFrame. Needs
   `zero.one/fxl` on the classpath.

   Example options:
   ```clojure
   {:header true :sheet \"Sheet2\"}
   ```"
  (fn [head & _] (class head)))
(defmethod read-xlsx! :default
  ([path] (read-xlsx! @defaults/spark path))
  ([path options] (read-xlsx! @defaults/spark path options)))
(defmethod read-xlsx! SparkSession
  ([spark path] (read-xlsx! spark path {:header true}))
  ([spark path options]
   (let [cells     ((fxl "read-xlsx!") path)
         table     ((fxl "cells->table") cells (:sheet options))
         col-names (if (:header options)
                     (first table)
                     (map #(str "_c" %) (-> table first count range)))
         table     (if (:header options) (rest table) table)
         dataset   (dataset-creation/table->dataset spark table col-names)]
     (-> dataset
         (cond-> (:kebab-columns options) ->kebab-columns)))))

; Any data source
(defmulti read!
  "Loads a DataFrame from any data source, as Spark's DataFrameReader does.
  The options map takes `:format`, such as `\"parquet\"` or `\"delta\"`
  (Spark's `spark.sql.sources.default` without it), `:path` or `:paths`,
  `:schema`, as for the other readers, and `:kebab-columns`. Every other key is
  a reader option, as for the other readers: a keyword key in camelCase, such
  as `:version-as-of`, and a string key as it is, such as `\"snapshot-id\"`.
  Without a path, it loads what the options name, as a JDBC source does.

  ```clojure
  (g/read! {:format \"delta\" :path \"/data/events\" :version-as-of 3})
  (g/read! spark {:format \"csv\" :paths [\"a.csv\" \"b.csv\"] :header true})
  ```"
  (fn [head & _] (class head)))
(defmethod read! :default
  [options]
  (read! @defaults/spark options))
(defmethod read! SparkSession
  [spark options]
  (let [{:keys [format path paths schema kebab-columns]} options
        _      (when (and path paths)
                 (throw (ex-info "read! takes :path or :paths, not both." {:options options})))
        reader (-> (.read spark)
                   (cond-> format (.format (name format)))
                   (cond-> schema (.schema (dataset-creation/->schema schema)))
                   (configure-reader-or-writer
                    (dissoc options :format :path :paths :schema :kebab-columns)))
        loaded (cond
                 path  (.load reader ^String path)
                 paths (.load reader ^"[Ljava.lang.String;" (into-array String paths))
                 :else (.load reader))]
    (cond-> loaded kebab-columns ->kebab-columns)))

(defn write!
  "Saves the DataFrame to any data source, as Spark's DataFrameWriter does.
  The options map takes `:format` (Spark's `spark.sql.sources.default` without
  it), `:path`, `:mode`, one of `:append`, `:overwrite`, `:error`, the default,
  and `:ignore`, and `:partition-by`. Every other key is a writer option, as
  for the other writers. Without a path, it saves to what the options name, as
  a JDBC source does. `:bucket-by` and `:sort-by` need `write-table!`.

  ```clojure
  (g/write! dataframe {:format \"delta\" :path \"/data/events\" :mode :append})
  ```"
  [dataframe options]
  (let [path   (:path options)
        writer (configure-base-writer (.write dataframe) (dissoc options :path))]
    (if path
      (.save writer ^String path)
      (.save writer))))

(defn- parse-strings [format-name dataframe col-name options]
  (let [{:keys [schema kebab-columns]} options
        strings (-> (dataset/select dataframe col-name) (.as (Encoders/STRING)))
        reader  (-> (.. dataframe sparkSession read)
                    (cond-> schema (.schema (dataset-creation/->schema schema)))
                    (configure-reader-or-writer (dissoc options :schema :kebab-columns)))
        parsed  (case format-name
                  :json (.json reader strings)
                  :csv  (.csv reader strings))]
    (cond-> parsed kebab-columns ->kebab-columns)))

(defn parse-json
  "With a DataFrame, parses the JSON strings in the column `col-name` into a
  DataFrame, as `read-json!` reads a file, with a row for each string. The
  options are `read-json!`'s, `:schema` included; without one, Spark infers
  the schema from the strings.

  With only a column, it's Spark's `parse_json` function, which parses a JSON
  string into a VARIANT, and needs Spark 4.0.

  ```clojure
  (g/parse-json events :payload {:schema \"id BIGINT, kind STRING\"})
  (g/select events {:payload (g/parse-json :payload)})
  ```"
  ([expr]
   (function-table/invoke "parse_json" [4 0] 'parse-json [expr]))
  ([dataframe col-name] (parse-json dataframe col-name {}))
  ([dataframe col-name options] (parse-strings :json dataframe col-name options)))

(defn parse-csv
  "Parses the CSV lines in the column `col-name` into a DataFrame, with a row
  for each line. The options are Spark's CSV options, plus `:schema`. Unlike
  `read-csv!`, it takes Spark's defaults, with no header and every column a
  string, unless the options say otherwise.

  ```clojure
  (g/parse-csv lines :line {:schema \"id INT, name STRING\"})
  ```"
  ([dataframe col-name] (parse-csv dataframe col-name {}))
  ([dataframe col-name options] (parse-strings :csv dataframe col-name options)))

(defmulti read-changes!
  "Reads a table's change feed: the rows that changed between the versions or
  timestamps that the options give, such as `:starting-version` and
  `:ending-version`, as Delta Lake and Iceberg tables have it. Every key is a
  reader option, as for `read-table!`. Needs Spark 4.2, and a catalog that
  supports change data capture, which Spark's built-in one doesn't.

  ```clojure
  (g/read-changes! \"lake.orders\" {:starting-version 3 :ending-version 9})
  ```"
  (fn [head & _] (class head)))
(defmethod read-changes! :default
  ([table-name] (read-changes! @defaults/spark table-name {}))
  ([table-name options] (read-changes! @defaults/spark table-name options)))
(defmethod read-changes! SparkSession
  ([spark table-name] (read-changes! spark table-name {}))
  ([spark table-name options]
   (spark/require-version! [4 2] "read-changes!")
   (-> (.read spark)
       (configure-reader-or-writer (dissoc options :kebab-columns))
       (.changes (name table-name))
       (cond-> (:kebab-columns options) ->kebab-columns))))

(defn- table-function-name [fn-name]
  (let [sql-name (string/replace (name fn-name) "-" "_")]
    (if (re-matches #"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*" sql-name)
      sql-name
      (throw (ex-info (str "Expected a table-valued function's name, such as :explode. Got: "
                           (pr-str fn-name))
                      {:name fn-name})))))

(defn table-function
  "Calls a table-valued function, such as `:range`, `:explode`, `:inline`,
  `:stack` or `:sql-keywords`, with `args`, and returns its table as a
  DataFrame. The args go to Spark as named SQL parameters, so they're what
  `g/sql` takes: a vector becomes an array, and from Spark 4.0, a column can
  build the array of structs that `:inline` takes.

  ```clojure
  (g/table-function :explode [[1 2 3]])
  (g/table-function spark :stack [(int 2) 1 \"a\" 2 \"b\"])
  (g/table-function :inline [(g/array (g/struct (g/as (g/lit 1) :id)))])
  ```"
  {:arglists '([fn-name] [fn-name args] [spark fn-name] [spark fn-name args])}
  [& spark-name-and-args]
  (let [[spark fn-name args] (if (instance? SparkSession (first spark-name-and-args))
                               spark-name-and-args
                               (cons @defaults/spark spark-name-and-args))
        params               (map #(keyword (str "arg" %)) (range (count args)))]
    (spark/sql spark
               (str "SELECT * FROM " (table-function-name fn-name)
                    "(" (string/join ", " (map #(str (keyword %)) params)) ")")
               (zipmap params args))))

; Hive/Managed Tables
(defmulti read-table!
  "Reads a managed (hive) table and returns the result as a DataFrame. A map
  of reader options can follow the table's name, and `:kebab-columns` in it
  renames the columns as for the other readers."
  (fn [head & _] (class head)))
(defmethod read-table! :default
  ([table-name] (read-table! @defaults/spark table-name))
  ([table-name options] (read-table! @defaults/spark table-name options)))
(defmethod read-table! SparkSession
  ([spark table-name] (.table spark (name table-name)))
  ([spark table-name options]
   (-> (.read spark)
       (configure-reader-or-writer (dissoc options :kebab-columns))
       (.table (name table-name))
       (cond-> (:kebab-columns options) ->kebab-columns))))

(defn write-table!
  "Writes the dataset to a managed (hive) table. The options take `:format`,
  `:mode`, `:partition-by`, `:bucket-by` with the number of buckets and the
  columns, such as `[8 :id]` or `[8 :id :day]`, `:sort-by` for the columns to
  sort each bucket by, and `:cluster-by` for the clustering columns
  (Spark 4.0). Every other key is a writer option.

  ```clojure
  (g/write-table! dataframe \"sales\" {:format :parquet :bucket-by [8 :id] :sort-by :day})
  ```"
  ([^Dataset dataframe table-name]
   (write-table! dataframe table-name {}))
  ([^Dataset dataframe table-name options]
   (-> dataframe
       (.write)
       (configure-base-writer options)
       (.saveAsTable (name table-name)))))

(defn insert-into!
  "Inserts the dataset's rows into an existing table, matching the columns by
  position, not by name, as Spark's `insertInto` does. With
  `{:overwrite true}`, the rows replace the table's.

  ```clojure
  (g/insert-into! dataframe \"sales\")
  (g/insert-into! dataframe \"sales\" {:overwrite true})
  ```"
  ([dataframe table-name] (insert-into! dataframe table-name {}))
  ([dataframe table-name {:keys [overwrite]}]
   (-> (.write dataframe)
       (cond-> (some? overwrite) (.mode (if overwrite "overwrite" "append")))
       (.insertInto (name table-name)))))

(def ^:private write-to-modes
  #{:create :replace :create-or-replace :append :overwrite :overwrite-partitions})

(defn- ->option-string [v]
  (if (keyword? v) (name v) (str v)))

(defn write-to!
  "Writes the dataset to a table through Spark's DataFrameWriterV2, the
  `writeTo` API, for catalogs such as Delta's and Iceberg's. The options take
  `:mode`, which is required: `:create`, `:replace`, `:create-or-replace`,
  `:append`, `:overwrite`, which replaces the rows that the column
  `:condition` holds for, or `:overwrite-partitions`. When it creates a table,
  `:using` is its format, `:partitioned-by` its partition columns or
  transforms, `:cluster-by` its clustering columns (Spark 4.0), and
  `:table-properties` a map of its properties. Every other key is a writer
  option. Spark's built-in session catalog only takes `:create`.

  ```clojure
  (g/write-to! dataframe \"lake.events\" {:mode :create :using \"delta\" :partitioned-by [:day]})
  (g/write-to! dataframe \"lake.events\" {:mode :overwrite :condition (g/=== :day \"2026-10-01\")})
  ```"
  [dataframe table-name options]
  (let [{:keys [mode using partitioned-by cluster-by table-properties condition]} options
        mode (some-> mode keyword)]
    (when cluster-by (spark/require-version! [4 0] "write-to! with :cluster-by"))
    (when-not (write-to-modes mode)
      (throw (ex-info (str "write-to! takes a :mode, one of " (sort write-to-modes)
                           ". Got: " (pr-str (:mode options)))
                      {:mode (:mode options)})))
    (when (and (= :overwrite mode) (nil? condition))
      (throw (ex-info "write-to! with :mode :overwrite takes a :condition: the rows to replace."
                      {:options options})))
    (let [[partition & partitions] (when partitioned-by
                                     (column/->col-array (ensure-coll partitioned-by)))
          writer (-> (.writeTo dataframe (name table-name))
                     (cond-> using (.using (name using)))
                     (configure-reader-or-writer
                      (dissoc options :mode :using :partitioned-by :cluster-by :table-properties
                              :condition))
                     (cond-> partition (.partitionedBy partition (into-array Column partitions)))
                     (cond-> cluster-by (cluster-writer cluster-by)))
          writer (reduce (fn [w [k v]] (.tableProperty w (->option-string k) (->option-string v)))
                         writer
                         table-properties)]
      (case mode
        :create               (.create writer)
        :replace              (.replace writer)
        :create-or-replace    (.createOrReplace writer)
        :append               (.append writer)
        :overwrite            (.overwrite writer (column/->column condition))
        :overwrite-partitions (.overwritePartitions writer)))))

(defn create-temp-view!
  "Creates a local temporary view using the given name.

  Local temporary view is session-scoped. Its lifetime is the lifetime of the session that
  created it, i.e. it will be automatically dropped when the session terminates. It's not tied
  to any databases, i.e. we can't use `db1.view1` to reference a local temporary view."
  [^Dataset dataframe ^String view-name]
  (.createTempView dataframe view-name))

(defn create-or-replace-temp-view!
  "Creates or replaces a local temporary view using the given name.

  The lifetime of this temporary view is tied to the `SparkSession` that was used to create this Dataset."
  [^Dataset dataframe ^String view-name]
  (.createOrReplaceTempView dataframe view-name))

(defn create-global-temp-view!
  "Creates a global temporary view using the given name.

  Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
  i.e. it will be automatically dropped when the application terminates. It's tied to a system
  preserved database `global_temp`, and we must use the qualified name to refer a global temp
  view, e.g. `SELECT * FROM global_temp.view1`."
  [^Dataset dataframe ^String view-name]
  (.createGlobalTempView dataframe view-name))

(defn create-or-replace-global-temp-view!
  "Creates or replaces a global temporary view using the given name.

  Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
  i.e. it will be automatically dropped when the application terminates. It's tied to a system
  preserved database `global_temp`, and we must use the qualified name to refer a global temp
  view, e.g. `SELECT * FROM global_temp.view1`."
  [^Dataset dataframe ^String view-name]
  (.createOrReplaceGlobalTempView dataframe view-name))
