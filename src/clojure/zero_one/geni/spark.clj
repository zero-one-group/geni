(ns zero-one.geni.spark
  (:require
   [clojure.string :as string]
   [clojure.walk :as walk]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang Reflector)
   (org.apache.spark SparkConf SparkContext)
   (org.apache.spark.sql SparkSession)
   (scala Option)))

(defn- spark-context-or-nil
  "The session's SparkContext, or nil for a Spark Connect session, which
  doesn't have one."
  ^SparkContext [^SparkSession spark]
  (try
    (.sparkContext spark)
    (catch UnsupportedOperationException _ nil)))

(defn spark-context
  "The session's SparkContext. Only classic sessions have one: for a Spark
  Connect session, it throws an error that says so."
  ^SparkContext [^SparkSession spark]
  (or (spark-context-or-nil spark)
      (throw (ex-info (str "This needs a classic SparkSession, with a SparkContext. A Spark Connect "
                           "session has none, so RDDs, broadcasts and MLlib don't work over it.")
                      {:session spark}))))

(defn connect-only?
  "Whether the Spark Connect client is on the classpath without classic
  Spark, as it is when Spark 4's spark-connect-client-jvm takes the place of
  spark-sql."
  []
  (boolean (and (class-named "org.apache.spark.sql.connect.SparkSession")
                (not (class-named "org.apache.spark.sql.classic.SparkSession")))))

(defn- running [^Option session]
  (when (.isDefined session)
    (let [^SparkSession session (.get session)]
      ;; Spark 4 only returns sessions that are still usable. Spark 3.5
      ;; returns stopped ones too.
      (when-not (some-> (spark-context-or-nil session) .isStopped)
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
  (when (connect-only?)
    (throw (ex-info (str "create-spark-session starts classic Spark, but only the Spark Connect "
                         "client is on the classpath. Use g/connect to connect to a server.")
                    {})))
  (let [preset   (SparkConf.)
        builder  (cond-> (SparkSession/builder)
                   (or app-name (not (.contains preset "spark.app.name")))
                   (.appName (or app-name "Geni App"))

                   (or master (not (.contains preset "spark.master")))
                   (.master (or master "local[*]")))
        builder  (reduce (fn [b [k v]] (.config b (name k) v)) builder configs)
        created? (nil? (active-session))
        session  (.getOrCreate builder)
        context  (if (or log-level checkpoint-dir)
                   (spark-context session)
                   (spark-context-or-nil session))]
    (cond
      log-level                                   (.setLogLevel context log-level)
      (and created? context (spark-log-profile?)) (.setLogLevel context "WARN"))
    (when checkpoint-dir
      (.setCheckpointDir context checkpoint-dir))
    session))

(defn connect-session
  "A new session on a Spark Connect server, which becomes Spark's default and
  active session. See `g/connect`, which also makes Geni use it."
  ^SparkSession [url {:keys [configs]}]
  (when-not (class-named "org.apache.spark.sql.connect.SparkSession")
    (throw (ex-info (str "Spark Connect needs Spark 4's JVM client, "
                         "org.apache.spark/spark-connect-client-jvm_2.13, on the classpath "
                         "in place of spark-sql.")
                    {:url url})))
  (let [builder (-> (SparkSession/builder) (.config "spark.api.mode" "connect"))
        builder (if url (.remote builder url) builder)
        builder (reduce (fn [b [k v]] (.config b (name k) v)) builder configs)
        ;; Not getOrCreate, which can return a session that was closed.
        session (.create builder)]
    (SparkSession/setDefaultSession session)
    (SparkSession/setActiveSession session)
    session))

(defn spark-conf
  "The session's Spark configs, as a map with keyword keys: Spark's
  `spark.conf().getAll()`. For a classic session, these are the SparkConf's
  settings, plus the SQL configs set on the session since. It works for Spark
  Connect sessions too.

  ```clojure
  (:spark.app.name (g/spark-conf spark))
  => \"Geni App\"
  ```"
  [^SparkSession spark-session]
  (-> spark-session .conf .getAll interop/scala-map->map walk/keywordize-keys))

(defn sql
  "Executes a SQL query using Spark, returning the result as a `DataFrame`.

  The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'.

  ```clojure
  (g/sql spark \"SELECT * FROM my_table\")
  ```"
  [^SparkSession spark ^String sql-text]
  (. spark sql sql-text))

