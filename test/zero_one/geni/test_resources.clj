(ns zero-one.geni.test-resources
  (:require
   [clojure.string :refer [split-lines split] :as string]
   [zero-one.geni.core :as g]
   [zero-one.geni.defaults]
   [clojure.java.io :as io])
  (:import
   (java.io File)
   (org.apache.spark.sql Dataset SparkSession)
   (java.nio.file.attribute FileAttribute)
   (java.nio.file Files Paths)
   (java.util UUID)))

(def spark zero-one.geni.defaults/spark)

;; Geni's default session sets Spark's log level to WARN, and the tests only
;; want errors.
(.setLogLevel (.sparkContext ^SparkSession @spark) "ERROR")

(def ^:private fixtures (atom {}))

(defn- per-session
  "Builds a fixture once per Spark session, since `reset-session!` replaces
  the session and closes the old one."
  [k build]
  (let [session @spark
        [built-for value] (get @fixtures k)]
    (if (identical? built-for session)
      value
      (let [value (build session)]
        (swap! fixtures assoc k [session value])
        value))))

(defn- local-copy
  "The same rows in memory, in one partition like the `limit` they come from,
  so that Spark doesn't scan any files to query them."
  [^Dataset df]
  (-> (.createDataFrame (.sparkSession df) (.collectAsList df) (.schema df))
      (.coalesce 1)))

(defn melbourne-df []
  (per-session :melbourne
               #(g/read-parquet! % "test/resources/melbourne_housing_snapshot.parquet")))

(defn df-1 []
  (per-session :df-1 (fn [_] (local-copy (g/limit (melbourne-df) 1)))))

(defn df-20 []
  (per-session :df-20 (fn [_] (local-copy (g/limit (melbourne-df) 20)))))

(defn df-50 []
  (per-session :df-50 (fn [_] (local-copy (g/limit (melbourne-df) 50)))))

(defn libsvm-df []
  (per-session :libsvm
               #(g/cache (g/read-libsvm! % "test/resources/sample_libsvm_data.txt" {:num-features "780"}))))

(defn k-means-df []
  (per-session :k-means
               #(g/cache (g/read-libsvm! % "test/resources/sample_kmeans_data.txt" {:num-features "780"}))))

(defn ratings-df []
  (per-session :ratings
               (fn [session]
                 (->> (slurp "test/resources/sample_movielens_ratings.txt")
                      split-lines
                      (map #(split % #"::"))
                      (map (fn [row]
                             {:user-id   (Integer/parseInt (first row))
                              :movie-id  (Integer/parseInt (second row))
                              :rating    (Float/parseFloat (nth row 2))
                              :timestamp (long (Integer/parseInt (nth row 3)))}))
                      (g/records->dataset session)
                      g/cache))))

(def -tmp-dir-attr
  (into-array FileAttribute '()))

(defn create-temp-dir! ^File []
  (.toFile (Files/createTempDirectory "tmp-dir-" -tmp-dir-attr)))

(defn create-temp-file! ^File [extension]
  (let [temp-dir (create-temp-dir!)]
    (File/createTempFile "temporary" extension temp-dir)))

(defn recursive-delete-dir
  [^File file]
  (when (.isDirectory file)
    (doseq [file-in-dir (.listFiles file)]
      (recursive-delete-dir file-in-dir)))
  (io/delete-file file))

(defn delete-warehouse!
  []
  (let [wh-path (-> @spark .conf (.get "spark.sql.warehouse.dir") (string/replace "file:" ""))
        wh-dir (File. wh-path)]
    (when (.exists wh-dir)
      (recursive-delete-dir wh-dir))))

(def test-warehouses-root
  (str (Paths/get (.getAbsolutePath (io/file "")) (into-array String ["spark-warehouses"]))))

(defn rand-wh-path
  []
  (str "file:" (Paths/get test-warehouses-root (into-array String [(str (UUID/randomUUID))]))))

(defn reset-session!
  []
  (.close @spark)
  (reset! spark (g/create-spark-session
                 (-> zero-one.geni.defaults/session-config
                     (assoc-in [:configs :spark.sql.warehouse.dir] (rand-wh-path))
                     (assoc :log-level "ERROR")))))

(defmacro with-fresh-session
  "Runs `body` in a new Spark session with its own warehouse, and deletes the
  warehouse afterwards."
  [& body]
  `(do
     (reset-session!)
     (try
       ~@body
       (finally
         (delete-warehouse!)))))
