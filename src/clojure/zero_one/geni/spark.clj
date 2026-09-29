(ns zero-one.geni.spark
  (:require
   [clojure.string :as string]
   [clojure.walk]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop])
  (:import
   (clojure.lang Reflector)
   (org.apache.spark SparkConf)
   (org.apache.spark.sql SparkSession)
   (scala Option)))

(defn- running [^Option session]
  (when (.isDefined session)
    (let [^SparkSession session (.get session)]
      (when-not (.. session sparkContext isStopped)
        session))))

(defn active-session
  "Spark's active SparkSession on this thread, or else its default one, as
  long as it's still running. Returns nil rather than creating a session."
  ^SparkSession []
  (or (running (SparkSession/getActiveSession))
      (running (SparkSession/getDefaultSession))))

(defn- log4j2-config-location
  "Where log4j2 read its config from. Nil when log4j2 isn't the logging
  backend, which is why this goes through reflection."
  []
  (try
    (let [call #(Reflector/invokeInstanceMethod %1 %2 (object-array 0))]
      (-> (Reflector/invokeStaticMethod "org.apache.logging.log4j.LogManager"
                                        "getContext"
                                        (object-array [false]))
          (call "getConfiguration")
          (call "getConfigurationSource")
          (call "getLocation")))
    (catch Throwable _ nil)))

(defn- spark-log-profile?
  "Whether Spark fell back to its own log4j2 profile, which logs at INFO. It
  does that when there's no log4j2 config on the classpath."
  []
  (boolean (some-> (log4j2-config-location) (string/includes? "org/apache/spark/log4j2"))))

(defn create-spark-session
  "The entry point to programming Spark with the Dataset and DataFrame API.

  Like Spark's `SparkSession.builder().getOrCreate()`, it returns the running
  session if there is one, and creates one otherwise. The options are:

  - `:app-name` and `:master`, which default to \"Geni App\" and \"local[*]\"
    unless `spark.app.name` or `spark.master` are set already, for instance by
    spark-submit.
  - `:configs`, a map of Spark configs.
  - `:log-level`, such as \"ERROR\". Without it, Geni only sets \"WARN\", and
    only when it starts Spark and there's no log4j2 config on the classpath,
    as `spark-shell` does.
  - `:checkpoint-dir`, the SparkContext's checkpoint directory.

  ```clojure
  (g/create-spark-session {:app-name \"My App\"
                           :configs  {:spark.sql.shuffle.partitions 8}})
  ```"
  ^SparkSession
  [{:keys [app-name master configs log-level checkpoint-dir]}]
  (let [preset   (SparkConf.)
        builder  (cond-> (SparkSession/builder)
                   (or app-name (not (.contains preset "spark.app.name")))
                   (.appName (or app-name "Geni App"))

                   (or master (not (.contains preset "spark.master")))
                   (.master (or master "local[*]")))
        builder  (reduce (fn [b [k v]] (.config b (name k) v)) builder configs)
        created? (nil? (active-session))
        session  (.getOrCreate builder)
        context  (.sparkContext session)]
    (cond
      log-level                           (.setLogLevel context log-level)
      (and created? (spark-log-profile?)) (.setLogLevel context "WARN"))
    (when checkpoint-dir
      (.setCheckpointDir context checkpoint-dir))
    session))

(defn spark-conf [spark-session]
  (->> spark-session
       .sparkContext
       .getConf
       interop/spark-conf->map))

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
