(ns zero-one.geni.data-sources
  (:refer-clojure :exclude [partition-by sort-by])
  (:require
    [camel-snake-kebab.core :refer [->camelCase]]))

;; TODO: read-edn!, write-edn!

(defn configure-reader-or-writer [unconfigured options]
  (reduce
    (fn [r [k v]] (.option r (->camelCase (name  k)) v))
    unconfigured
    options))

(defn read-data! [format spark path options]
  (let [unconfigured-reader (.. spark read (format format))
        configured-reader   (configure-reader-or-writer unconfigured-reader options)]
    (.load configured-reader path)))

(defn read-avro!
  ([spark path] (read-avro! spark path {}))
  ([spark path options] (read-data! "avro" spark path options)))

(defn read-parquet!
  ([spark path] (read-parquet! spark path {}))
  ([spark path options] (read-data! "parquet" spark path options)))

(def default-options
  {:csv {"header" "true"}})

(defn read-csv!
  ([spark path] (read-csv! spark path (:csv default-options)))
  ([spark path options]
   (read-data! "csv" spark path (merge (:csv default-options) options))))

(defn read-libsvm!
  ([spark path] (read-libsvm! spark path {}))
  ([spark path options] (read-data! "libsvm" spark path options)))

(defn read-json!
  ([spark path] (read-json! spark path {}))
  ([spark path options] (read-data! "json" spark path options)))

(defn read-text!
  ([spark path] (read-text! spark path {}))
  ([spark path options] (read-data! "text" spark path options)))

(defn read-jdbc! [spark options]
  (let [unconfigured-reader (.. spark sqlContext read (format "jdbc"))
        configured-reader   (configure-reader-or-writer unconfigured-reader options)]
    (.load configured-reader)))

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
