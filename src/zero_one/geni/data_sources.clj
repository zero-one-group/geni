(ns zero-one.geni.data-sources
  (:refer-clojure :exclude [partition-by sort-by])
  (:require
    [camel-snake-kebab.core :refer [->camelCase]]
    [zero-one.geni.defaults])
  (:import
    (org.apache.spark.sql SparkSession)))

;; TODO: read-edn!, write-edn!

(def default-spark zero-one.geni.defaults/spark)

(defn configure-reader-or-writer [unconfigured options]
  (reduce
    (fn [r [k v]] (.option r (->camelCase (name  k)) v))
    unconfigured
    options))

(def default-options
  {"csv" {"header" "true"}})

(defn read-data! [format-name spark path options]
  (let [defaults            (default-options format-name)
        unconfigured-reader (.. spark read (format format-name))
        configured-reader   (configure-reader-or-writer
                              unconfigured-reader
                              (merge defaults options))]
    (.load configured-reader path)))

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

(defn write-data! [format dataframe path options]
  (let [mode                (:mode options)
        unconfigured-writer (-> dataframe
                                (.write)
                                (.format format)
                                (cond-> mode (.mode mode)))
        configured-writer   (configure-reader-or-writer
                              unconfigured-writer
                              (dissoc options :mode))]
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
