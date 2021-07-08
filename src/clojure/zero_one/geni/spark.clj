(ns zero-one.geni.spark
  (:require
   [clojure.walk]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop])
  (:import
   (org.apache.spark.sql SparkSession)
   (org.apache.log4j Logger Level)))

(defn- non-verbose-get-or-create-session [builder log-level]
  (let [logger         (Logger/getLogger "org")
        original-level (.getLevel logger)
        session        (do
                         (.setLevel logger (Level/toLevel log-level))
                         (.getOrCreate builder))]
    (.setLevel logger original-level)
    session))

(defn create-spark-session
  "The entry point to programming Spark with the Dataset and DataFrame API."
  [{:keys [app-name master configs log-level checkpoint-dir]
    :or   {app-name  "Geni App"
           master    "local[*]"
           configs   {}
           log-level "WARN"}}]
  (let [unconfigured (.. (SparkSession/builder)
                         (appName app-name)
                         (master master))
        configured   (reduce
                      (fn [s [k v]] (.config s (name k) v))
                      unconfigured
                      configs)
        session      (non-verbose-get-or-create-session configured log-level)
        context      (.sparkContext session)]
    (.setLogLevel context log-level)
    (when checkpoint-dir
      (.setCheckpointDir context checkpoint-dir))
    session))

(defn spark-conf [spark-session]
  (->> spark-session
       .sparkContext
       .getConf
       interop/spark-conf->map))

(defn set-settings
  "Set the given spark session settings. Return the spark-session.

  The spark-session configuration is mutable, and thus these settings are applied in-place.
  "
  [spark-session settings]
  (reduce-kv (fn [sess k v] (-> sess .conf (.set (name k) v)))
             spark-session
             settings))

(defn with-settings
  "Temporarily sets the given spark session `settings`, runs the function `f`,
  and then reverts the spark session settings to their original values.

  `The function `f` should take 1 argument, the spark session with the temporary
  settings set.

  This is an unsafe operation when sharing a single spark session across multiple
  threads."
  [spark-session settings f]
  (let [current-settings (select-keys (spark-conf spark-session) (keys settings))
        session-with-settings (set-settings spark-session settings)
        result (f session-with-settings)]
    (set-settings session-with-settings current-settings)
    result))

(defn sql
  "Executes a SQL query using Spark, returning the result as a `DataFrame`.

  The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'.

  ```clojure
  (g/sql spark \"SELECT * FROM my_table\")
  ```"
  [^SparkSession spark ^String sql-text]
  (. spark sql sql-text))

;; Docs
(docs/add-doc!
 (var spark-conf)
 (-> docs/spark-docs :methods :spark :context :get-conf))
