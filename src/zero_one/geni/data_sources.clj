(ns zero-one.geni.data-sources
  (:refer-clojure :exclude [partition-by sort-by]))

(defn read-data! [format spark-session path options]
  (let [unconfigured-reader (.. spark-session read (format format))
        configured-reader   (reduce
                              (fn [r [k v]] (.option r k v))
                              unconfigured-reader
                              options)]
    (.load configured-reader path)))

(defn read-parquet!
  ([spark-session path] (read-parquet! spark-session path {}))
  ([spark-session path options] (read-data! "parquet" spark-session path options)))

(def default-options
  {:csv {"header" "true"}})

(defn read-csv!
  ([spark-session path] (read-csv! spark-session path (:csv default-options)))
  ([spark-session path options]
   (read-data! "csv" spark-session path (merge options (:csv default-options)))))

(defn read-libsvm!
  ([spark-session path] (read-libsvm! spark-session path {}))
  ([spark-session path options] (read-data! "libsvm" spark-session path options)))

(defn read-json!
  ([spark-session path] (read-json! spark-session path {}))
  ([spark-session path options] (read-data! "json" spark-session path options)))

(defn read-text!
  ([spark-session path] (read-text! spark-session path {}))
  ([spark-session path options] (read-data! "text" spark-session path options)))

(defn write-data! [format dataframe path options]
  (let [mode                (:mode options)
        unconfigured-writer (-> dataframe
                                (.write)
                                (.format format)
                                (cond-> mode (.mode mode)))
        configured-writer   (reduce
                              (fn [w [k v]] (.option w k v))
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
