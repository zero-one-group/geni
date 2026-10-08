(ns zero-one.geni.test-resources
  (:require
   [clojure.string :refer [split-lines split] :as string]
   [zero-one.geni.core :as g]
   [zero-one.geni.core.udf-artifacts :as udf-artifacts]
   [zero-one.geni.defaults]
   [zero-one.geni.spark]
   [zero-one.geni.utils :refer [class-named]]
   [clojure.java.io :as io])
  (:import
   (clojure.lang Reflector)
   (java.io File)
   (org.apache.spark.sql Dataset SparkSession)
   (java.nio.file.attribute FileAttribute)
   (java.nio.file Files Paths)
   (java.util UUID)))

(def spark zero-one.geni.defaults/spark)

(defn spark-at-least?
  "Whether the Spark on the classpath, the Spark Connect client's over Spark
  Connect, is at least `version`, such as \"4.1\"."
  [version]
  (let [needed (mapv parse-long (split version #"\."))
        actual (mapv parse-long (re-seq #"\d+" (zero-one.geni.spark/classpath-version)))]
    (not (neg? (compare (vec (take (count needed) actual)) needed)))))

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

(defn stop-session!
  "Closes the running session, if there is one."
  []
  (some-> (zero-one.geni.spark/active-session) .close))

(defn connect?
  "Whether the tests run over Spark Connect, with Spark's JVM client on the
  classpath in place of classic Spark."
  []
  (zero-one.geni.spark/connect-only?))

;; Over Spark Connect, Clojure keeps the classes that it compiles from here
;; on, as g/connect's :keep-classes has it do, so that the functions of the
;; test namespaces that load after this one can go to the server as UDFs.
(when (connect?)
  (udf-artifacts/keep-classes!))

(defn clean-catalog!
  "Drops the databases, tables and global temp views that earlier tests left
  on the Spark Connect server, whose catalog outlives its sessions."
  []
  (let [session @spark
        show    #(g/collect (g/sql session %))]
    (doseq [{db :namespace} (show "SHOW DATABASES")
            :when (not= "default" db)]
      (g/sql session (str "DROP DATABASE `" db "` CASCADE")))
    (doseq [{:keys [tableName isTemporary]} (show "SHOW TABLES IN default")
            :when (not isTemporary)]
      (g/sql session (str "DROP TABLE IF EXISTS default.`" tableName "`")))
    (doseq [{:keys [namespace tableName]} (show "SHOW TABLES IN global_temp")
            :when (= "global_temp" namespace)]
      (g/sql session (str "DROP VIEW IF EXISTS global_temp.`" tableName "`")))))

(defn reset-session!
  "Replaces the running session with a new one that has its own warehouse.
  Geni's default session finds it, as Spark's active session. Over Spark
  Connect, the new session shares the server's catalog, which starts empty."
  []
  (stop-session!)
  (if (connect?)
    (do (g/connect) (clean-catalog!))
    (g/create-spark-session {:configs {:spark.sql.warehouse.dir (rand-wh-path)}})))

(defn observed-within
  "What g/observed gives for the observation, or :timed-out when it's still
  waiting after `ms`. A daemon thread waits, so that a failing test doesn't
  keep the JVM running."
  [observation ms]
  (let [result (promise)]
    (doto (Thread. #(deliver result (g/observed observation)))
      (.setDaemon true)
      (.start))
    (deref result ms :timed-out)))

(defn checkpoint-dir!
  "Gives the running session a checkpoint directory, which Geni's default
  session doesn't have, and returns the directory."
  []
  (let [dir "target/checkpoint/"]
    (.setCheckpointDir (.sparkContext ^SparkSession @spark) dir)
    dir))

(defmacro with-fresh-session
  "Runs `body` in a new Spark session with its own warehouse, and deletes the
  warehouse afterwards."
  [& body]
  `(do
     (reset-session!)
     (try
       ~@body
       (finally
         (if (connect?) (clean-catalog!) (delete-warehouse!))))))

(defn without-task-error-logs
  "Calls `f` with Spark's executor and scheduler logs off, for a check whose
  Spark job fails on purpose, which they'd log with a stack trace. Over Spark
  Connect, the server does that logging, so a client without log4j2's core
  just calls `f`."
  [f]
  (if-not (class-named "org.apache.logging.log4j.core.config.Configurator")
    (f)
    (let [call      #(Reflector/invokeStaticMethod ^String %1 ^String %2 (object-array %&))
          set-level #(call "org.apache.logging.log4j.core.config.Configurator" "setLevel" %1 %2)
          loggers   ["org.apache.spark.executor.Executor" "org.apache.spark.scheduler.TaskSetManager"]
          before    (mapv #(Reflector/invokeInstanceMethod
                            (call "org.apache.logging.log4j.LogManager" "getLogger" %)
                            "getLevel" (object-array 0))
                          loggers)]
      (try
        (doseq [logger loggers]
          (set-level logger (call "org.apache.logging.log4j.Level" "toLevel" "OFF")))
        (f)
        (finally
          (doseq [[logger level] (map vector loggers before)]
            (set-level logger level)))))))
