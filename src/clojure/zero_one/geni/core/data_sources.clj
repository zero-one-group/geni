(ns zero-one.geni.core.data-sources
  (:refer-clojure :exclude [partition-by])
  (:require
   [camel-snake-kebab.core :refer [->camelCase]]
   [clojure.edn :as edn]
   [clojure.string :as string]
   [clojure.java.io :as io]
   [jsonista.core :as jsonista]
   [zero-one.fxl.core :as fxl]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.core.dataset :as dataset]
   [zero-one.geni.utils :refer [ensure-coll]])
  (:import
   (java.text Normalizer Normalizer$Form)
   (org.apache.spark.sql SparkSession Dataset DataFrameWriter)))

(defn- configure-reader-or-writer [unconfigured options]
  (reduce
   (fn [r [k v]] (.option r (->camelCase (name  k)) v))
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
                         (map camel-snake-kebab.core/->kebab-case))]
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
         configured-reader   (configure-reader-or-writer unconfigured-reader options)]
     (.load configured-reader))))

(defn- partition-by-arg [partition-id]
  (into-array java.lang.String (map name (ensure-coll partition-id))))

(defn- configure-base-writer ^DataFrameWriter
  [writer options]
  (let [mode                (:mode options)
        partition-id        (:partition-by options)
        writer (-> writer
                   (cond-> mode (.mode mode))
                   (cond-> partition-id (.partitionBy (partition-by-arg partition-id))))]
    (configure-reader-or-writer writer (dissoc options :mode :partition-by))))

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
                                (cond-> mode (.mode mode)))
        configured-writer   (configure-reader-or-writer
                             unconfigured-writer
                             (dissoc options :mode))]
    (.save configured-writer)))

;; EDN
(defn- file-exists? [path]
  (.exists (io/file path)))

(defn- read-as-keywords [json-str]
  (jsonista/read-value json-str jsonista/keyword-keys-object-mapper))

(defn write-edn!
  "Writes an EDN file at the specified path."
  ([dataframe path] (write-edn! dataframe path {}))
  ([dataframe path options]
   (let [records   (->> dataframe
                        .toJSON
                        .collect
                        (mapv read-as-keywords))
         overwrite (if (= (:mode options) "overwrite") true false)]
     (if (and overwrite (file-exists? path))
       (spit path records)
       (throw (Exception. (format "path file:%s already exists!" path)))))))

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
(defn write-xlsx!
  "Writes an Excel file at the specified path."
  ([dataframe path] (write-xlsx! dataframe path {}))
  ([dataframe path options]
   (let [records   (dataset/collect dataframe)
         col-keys  (dataset/columns dataframe)
         overwrite (if (= (:mode options) "overwrite") true false)]
     (if (and overwrite (file-exists? path))
       (fxl/write-xlsx!
        (fxl/concat-below
         (fxl/row->cells (dataset/column-names dataframe))
         (fxl/records->cells col-keys records))
        path)
       (throw (Exception. (format "path file:%s already exists!" path)))))))

(defmulti read-xlsx!
  "Loads an Excel file and returns the results as a DataFrame.

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
   (let [cells     (fxl/read-xlsx! path)
         table     (fxl/cells->table cells (:sheet options))
         col-names (if (:header options)
                     (first table)
                     (map #(str "_c" %) (-> table first count range)))
         table     (if (:header options) (rest table) table)
         dataset   (dataset-creation/table->dataset spark table col-names)]
     (-> dataset
         (cond-> (:kebab-columns options) ->kebab-columns)))))

; Hive/Managed Tables
(defmulti read-table!
  "Reads a managed (hive) table and returns the result as a DataFrame."
  (fn [head & _] (class head)))
(defmethod read-table! :default
  ([table-name] (read-table! @defaults/spark table-name)))
(defmethod read-table! SparkSession
  ([spark table-name] (.table spark table-name)))

(defn write-table!
  "Writes the dataset to a managed (hive) table."
  ([^Dataset dataframe ^String table-name]
   (write-table! dataframe table-name {}))
  ([^Dataset dataframe ^String table-name options]
   (-> dataframe
       (.write)
       (configure-base-writer options)
       (.saveAsTable table-name))))

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


;; Delta
(defmulti read-delta!
  "Loads a delta table from a directory and returns the results as a DataFrame.

   Spark's DataFrameReader options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html
   See: https://docs.delta.io/latest/quick-start.html#read-data"
  (fn [head & _] (class head)))
(defmethod read-delta! :default
  ([path] (read-delta! @defaults/spark path))
  ([path options] (read-delta! @defaults/spark path options)))
(defmethod read-delta! SparkSession
  ([spark path] (read-delta! spark path {}))
  ([spark path options] (read-data! "delta" spark path options)))


(defn write-delta!
  "Writes a delta table at the specified path.

   Spark's DataFrameWriter options may be passed in as a map of options.

   See: https://spark.apache.org/docs/latest/sql-data-sources.html
   See: https://docs.delta.io/latest/quick-start.html#create-a-table"
  ([dataframe path] (write-delta! dataframe path {}))
  ([dataframe path options] (write-data! "delta" dataframe path options)))
