(ns zero-one.geni.core.data-sources
  (:refer-clojure :exclude [partition-by])
  (:require
    [camel-snake-kebab.core :refer [->camelCase]]
    [clojure.edn :as edn]
    [clojure.string :as string]
    [clojure.java.io :as io]
    [jsonista.core :as jsonista]
    [zero-one.fxl.core :as fxl]
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.core.dataset-creation :as dataset-creation]
    [zero-one.geni.core.dataset :as dataset]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (java.text Normalizer Normalizer$Form)
    (org.apache.spark.sql SparkSession)))

(def default-spark zero-one.geni.defaults/spark)

(defn configure-reader-or-writer [unconfigured options]
  (reduce
    (fn [r [k v]] (.option r (->camelCase (name  k)) v))
    unconfigured
    options))

(def default-options
  {"csv" {:header "true" :infer-schema "true"}})

(defn deaccent [string]
  ;; Source: https://gist.github.com/maio/e5f85d69c3f6ca281ccd
  (let [normalized (Normalizer/normalize string Normalizer$Form/NFD)]
    (string/replace normalized #"\p{InCombiningDiacriticalMarks}+" "")))

(defn remove-punctuations [string]
  (string/replace string #"[.,\/#!$%\^&\*;:{}=\`~()°]" ""))

(defn ->kebab-columns [dataset]
  (let [new-columns (->> dataset
                         .columns
                         (map remove-punctuations)
                         (map deaccent)
                         (map camel-snake-kebab.core/->kebab-case))]
    (.toDF dataset (interop/->scala-seq new-columns))))

(defn read-data! [format-name spark path options]
  (let [reader-opts (dissoc options :kebab-columns :schema)
        defaults    (default-options format-name)
        schema      (:schema options)
        reader      (-> (.. spark read (format format-name))
                        (configure-reader-or-writer (merge defaults reader-opts))
                        (cond-> (not (nil? schema))
                          (.schema (dataset-creation/->schema schema))))]
    (-> (.load reader path)
        (cond-> (:kebab-columns options) ->kebab-columns))))

(defmulti read-avro! (fn [head & _] (class head)))
(defmethod read-avro! :default
  ([path] (read-avro! @default-spark path))
  ([path options] (read-avro! @default-spark path options)))
(defmethod read-avro! SparkSession
  ([spark path] (read-avro! spark path {}))
  ([spark path options] (read-data! "avro" spark path options)))

(defmulti read-parquet! (fn [head & _] (class head)))
(defmethod read-parquet! :default
  ([path] (read-parquet! @default-spark path))
  ([path options] (read-parquet! @default-spark path options)))
(defmethod read-parquet! SparkSession
  ([spark path] (read-parquet! spark path {}))
  ([spark path options] (read-data! "parquet" spark path options)))

(defmulti read-csv! (fn [head & _] (class head)))
(defmethod read-csv! :default
  ([path] (read-csv! @default-spark path))
  ([path options] (read-csv! @default-spark path options)))
(defmethod read-csv! SparkSession
  ([spark path] (read-csv! spark path {}))
  ([spark path options] (read-data! "csv" spark path options)))

(defmulti read-libsvm! (fn [head & _] (class head)))
(defmethod read-libsvm! :default
  ([path] (read-libsvm! @default-spark path))
  ([path options] (read-libsvm! @default-spark path options)))
(defmethod read-libsvm! SparkSession
  ([spark path] (read-libsvm! spark path {}))
  ([spark path options] (read-data! "libsvm" spark path options)))

(defmulti read-json! (fn [head & _] (class head)))
(defmethod read-json! :default
  ([path] (read-json! @default-spark path))
  ([path options] (read-json! @default-spark path options)))
(defmethod read-json! SparkSession
  ([spark path] (read-json! spark path {}))
  ([spark path options] (read-data! "json" spark path options)))

(defmulti read-text! (fn [head & _] (class head)))
(defmethod read-text! :default
  ([path] (read-text! @default-spark path))
  ([path options] (read-text! @default-spark path options)))
(defmethod read-text! SparkSession
  ([spark path] (read-text! spark path {}))
  ([spark path options] (read-data! "text" spark path options)))

(defn read-jdbc!
  ([options] (read-jdbc! @default-spark options))
  ([spark options]
   (let [unconfigured-reader (.. spark sqlContext read (format "jdbc"))
         configured-reader   (configure-reader-or-writer unconfigured-reader options)]
     (.load configured-reader))))

(defn- partition-by-arg [partition-id]
  (into-array java.lang.String (map name (ensure-coll partition-id))))

(defn write-data! [format dataframe path options]
  (let [mode                (:mode options)
        partition-id        (:partition-by options)
        unconfigured-writer (-> dataframe
                                (.write)
                                (.format format)
                                (cond-> mode (.mode mode))
                                (cond-> partition-id
                                  (.partitionBy (partition-by-arg partition-id))))
        configured-writer   (configure-reader-or-writer
                              unconfigured-writer
                              (dissoc options :mode :partition-by))]
    (.save configured-writer path)))

(defn write-parquet!
  ([dataframe path] (write-parquet! dataframe path {}))
  ([dataframe path options] (write-data! "parquet" dataframe path options)))

(defn write-csv!
  ([dataframe path] (write-csv! dataframe path {"header" "true"}))
  ([dataframe path options] (write-data! "csv" dataframe path (merge options {"header" "true"}))))

(defn write-libsvm!
  ([dataframe path] (write-libsvm! dataframe path {}))
  ([dataframe path options] (write-data! "libsvm" dataframe path options)))

(defn write-json!
  ([dataframe path] (write-json! dataframe path {}))
  ([dataframe path options] (write-data! "json" dataframe path options)))

(defn write-text!
  ([dataframe path] (write-text! dataframe path {}))
  ([dataframe path options] (write-data! "text" dataframe path options)))

(defn write-avro!
  ([dataframe path] (write-avro! dataframe path {}))
  ([dataframe path options] (write-data! "avro" dataframe path options)))

(defn write-jdbc! [dataframe options]
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
(defn file-exists? [path]
  (.exists (io/file path)))

(defn read-as-keywords [json-str]
  (jsonista/read-value json-str jsonista/keyword-keys-object-mapper))

(defn write-edn!
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

(defmulti read-edn! (fn [head & _] (class head)))
(defmethod read-edn! :default
  ([path] (read-edn! @default-spark path))
  ([path options] (read-edn! @default-spark path options)))
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
;; TODO: add option to specify sheet
(defn write-xlsx!
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

(defn cells->table
  ([cells] (cells->table cells (first (map :sheet cells))))
  ([cells sheet]
   (let [sheet   (or sheet "Sheet1")
         cells   (->> cells
                      (filter #(= (-> % :coord (:sheet "Sheet1")) sheet))
                      (map #(update % :coord dissoc :sheet)))
         indexed (group-by :coord cells)]
     (vec (for [i (range (inc (fxl/max-row cells)))]
            (vec (for [j (range (inc (fxl/max-col cells)))]
                   (-> (indexed {:row i :col j}) first :value))))))))

(defmulti read-xlsx! (fn [head & _] (class head)))
(defmethod read-xlsx! :default
  ([path] (read-xlsx! @default-spark path))
  ([path options] (read-xlsx! @default-spark path options)))
(defmethod read-xlsx! SparkSession
  ([spark path] (read-xlsx! spark path {:header true}))
  ([spark path options]
   (let [cells (fxl/read-xlsx! path)
         table (cells->table cells (:sheet options))
         col-names (if (:header options)
                     (first table)
                     (map #(str "_c" %) (-> table first count range)))
         table     (if (:header options) (rest table) table)
         dataset   (dataset-creation/table->dataset spark table col-names)]
     (-> dataset
         (cond-> (:kebab-columns options) ->kebab-columns)))))
