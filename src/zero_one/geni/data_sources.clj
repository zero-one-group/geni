(ns zero-one.geni.data-sources)

;; TODO:
;; Writer: format, option, saveMode, partitionBy, bucketBy, sortBy
;; File Types: text

(defn read-data! [format spark-session path options]
  (let [unconfigured-reader (.. spark-session read (format format))
        configured-reader   (reduce
                              (fn [r [k v]] (.option r k v))
                              unconfigured-reader
                              options)]
    (.load configured-reader path)))

(defn read-parquet!
  ([spark-session path] (read-data! "parquet" spark-session path {}))
  ([spark-session path options] (read-data! "parquet" spark-session path options)))

(defn read-csv!
  ([spark-session path] (read-data! "csv" spark-session path {"header" "true"}))
  ([spark-session path options] (read-data! "csv" spark-session path options)))

(defn read-libsvm!
  ([spark-session path] (read-data! "libsvm" spark-session path {}))
  ([spark-session path options] (read-data! "libsvm" spark-session path options)))

(defn read-json!
  ([spark-session path] (read-data! "json" spark-session path {}))
  ([spark-session path options] (read-data! "json" spark-session path options)))

(defn write-data! [format dataframe path options]
  (let [unconfigured-writer (-> dataframe
                                (.write)
                                (.format format)
                                (cond-> (:mode options) (.mode (:mode options)))
                                (cond-> (:bucket-by options) (.bucketBy (:bucket-by options)))
                                (cond-> (:sort-by options) (.sortBy (:sort-by options)))
                                (cond-> (:partition-by options) (.partitionBy (:partition-by options))))
        configured-writer   (reduce
                              (fn [w [k v]] (.option w k v))
                              unconfigured-writer
                              (dissoc options :mode :bucket-by :sort-by :partition-by))]
    (.save configured-writer path)))

(defn write-parquet!
  ([dataframe path] (write-data! "parquet" dataframe path {}))
  ([dataframe path options] (write-data! "parquet" dataframe path options)))

(defn write-csv!
  ([dataframe path] (write-data! "csv" dataframe path {"header" "true"}))
  ([dataframe path options] (write-data! "csv" dataframe path (merge options {"header" "true"}))))

(defn write-libsvm!
  ([dataframe path] (write-data! "libsvm" dataframe path {}))
  ([dataframe path options] (write-data! "libsvm" dataframe path options)))

(defn write-json!
  ([dataframe path] (write-data! "json" dataframe path {}))
  ([dataframe path options] (write-data! "json" dataframe path options)))
