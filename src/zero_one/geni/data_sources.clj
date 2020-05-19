(ns zero-one.geni.data-sources)

(defn read-parquet! [spark-session path]
  (.. spark-session
      read
      (parquet path)))

(defn write-parquet! [dataframe path]
  (.. dataframe
      write
      (mode "overwrite")
      (parquet path)))

(defn write-csv! [dataframe path]
  (.. dataframe
      write
      (format "com.databricks.spark.csv")
      (option "header" "true")
      (mode "overwrite")
      (save path)))

(defn read-csv! [spark-session path]
  (.. spark-session
      read
      (format "csv")
      (option "header" "true")
      (load path)))

(defn read-libsvm! [spark-session path]
  (.. spark-session
      read
      (format "libsvm")
      (load path)))

(defn write-libsvm! [dataframe path]
  (.. dataframe
      write
      (format "libsvm")
      (mode "overwrite")
      (save path)))

(defn read-json!  [spark-session path]
  (.. spark-session
      read
      (format "json")
      (load path)))

(defn write-json! [dataframe path]
  (.. dataframe
      write
      (format "json")
      (mode "overwrite")
      (save path)))
