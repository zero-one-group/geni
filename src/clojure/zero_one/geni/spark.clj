(ns zero-one.geni.spark
  (:require
   [clojure.string :as string]
   [clojure.walk :as walk]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang DynamicClassLoader Reflector RT)
   (java.lang Module ModuleLayer)
   (org.apache.spark SparkConf SparkContext)
   (org.apache.spark.sql Column SparkSession functions)
   (org.slf4j LoggerFactory)
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

(defn classic-session?
  "Whether the session is classic Spark's rather than a Spark Connect one."
  [^SparkSession spark]
  (some? (spark-context-or-nil spark)))

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

(defn- quieten-spark-start!
  "Before Geni starts Spark on Spark's own log4j2 profile, which logs at INFO,
  sets the root level to WARN, so that Spark's INFO lines as it starts don't
  show. It has Spark initialise its logging first, without printing, when it
  hasn't yet: Spark 3.5 does so as it starts, and Spark 4 when the first Column
  is made. With a log4j2 config on the classpath, it does nothing."
  []
  (try
    (let [module  #(Reflector/getStaticField ^String % "MODULE$")
          call    #(Reflector/invokeInstanceMethod %1 %2 (object-array %3))
          logging (module "org.apache.spark.internal.Logging$")]
      (when (call logging "islog4j2DefaultConfigured" [])
        ;; SparkContext's companion object has Spark's Logging trait.
        (call (module "org.apache.spark.SparkContext$") "initializeLogIfNecessary" [false true]))
      (when (spark-log-profile?)
        (let [warn (Reflector/getStaticField "org.apache.logging.log4j.Level" "WARN")]
          (Reflector/invokeStaticMethod "org.apache.logging.log4j.core.config.Configurator"
                                        "setRootLevel"
                                        (object-array [warn])))))
    (catch Exception _ nil)))

(def ^:private launcher-opens
  "The --add-opens flags of Spark 4.2's launcher (its JavaModuleOptions), for
  the Spark Connect client, which comes without spark-launcher. Spark 3.5's
  are the same, and spark-setup-test checks them against the Spark it runs on."
  ["--add-opens=java.base/java.lang=ALL-UNNAMED"
   "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
   "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
   "--add-opens=java.base/java.io=ALL-UNNAMED"
   "--add-opens=java.base/java.net=ALL-UNNAMED"
   "--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/java.util=ALL-UNNAMED"
   "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
   "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
   "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
   "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
   "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
   "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"])

(defn- spark-opens
  "The --add-opens flags that Spark's launcher sets: its own list when
  spark-launcher is on the classpath, as it is with classic Spark."
  []
  (or (some->> (class-named "org.apache.spark.launcher.JavaModuleOptions")
               (#(Reflector/invokeStaticMethod ^Class % "defaultModuleOptions" (object-array 0)))
               (re-seq #"--add-opens=\S+")
               seq)
      launcher-opens))

(defn- missing-opens
  "The flags among `flags`, each `--add-opens=module/package=ALL-UNNAMED`, that
  this JVM wasn't started with. A module or package that this JVM doesn't have
  is skipped, as `sun.security.action` is from JDK 24, though Spark's launcher
  still opens it."
  [flags]
  (let [unnamed (.getModule RT)
        layer   (ModuleLayer/boot)]
    (remove (fn [flag]
              (if-let [[_ module-name package] (re-matches #"--add-opens=([^/]+)/([^=]+)=.*" flag)]
                (let [module (.findModule layer module-name)]
                  (or (not (.isPresent module))
                      (not (.contains (.getPackages ^Module (.get module)) package))
                      (.isOpen ^Module (.get module) package unnamed)))
                true))
            flags)))

(defn- missing-flags-message [missing]
  (str "The JVM lacks flags that Spark's launcher sets, which Spark needs on JDK 17 and later: "
       (string/join " " missing) ". The Spark setups in Geni's README have all of them, "
       "as :jvm-opts in deps.edn."))

(defn- warn!
  "Logs a warning, having Spark initialise its logging first, as Spark does
  before it logs. Otherwise a Spark Connect client, which hasn't logged yet,
  leaves log4j2 on its own default config, which only shows errors."
  [message]
  (try
    (let [companion (if (connect-only?)
                      "org.apache.spark.sql.connect.SparkSession$"
                      "org.apache.spark.SparkContext$")]
      ;; The companion object has Spark's Logging trait.
      (Reflector/invokeInstanceMethod (Reflector/getStaticField ^String companion "MODULE$")
                                      "initializeLogIfNecessary"
                                      (object-array [false true])))
    (catch Exception _ nil))
  (.warn (LoggerFactory/getLogger "zero-one.geni.spark") ^String message))

(def ^:private clojure-serializer
  "Java serialisation that reads booleans back as `true` and `false`."
  "zero_one.geni.rdd.ClojureSerializer")

(defn- local-master? [master]
  (boolean (some->> master (re-matches #"local(\[[^\]]*\])?"))))

(defn- get-or-create
  "Spark's `getOrCreate`, with a Clojure DynamicClassLoader as the thread's
  context class loader while Spark starts. The executors of a local session
  load classes through the loader they find there, and a DynamicClassLoader
  also finds the classes of functions defined at a REPL or in a script, which
  RDD functions and UDFs send them. REPLs such as nREPL's already use one."
  ^SparkSession [builder]
  (let [thread (Thread/currentThread)
        loader (.getContextClassLoader thread)]
    (if (instance? DynamicClassLoader loader)
      (.getOrCreate builder)
      (try
        (.setContextClassLoader thread (DynamicClassLoader. loader))
        (.getOrCreate builder)
        (finally
          (.setContextClassLoader thread loader))))))

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
    as `spark-shell` does, before Spark logs its INFO lines as it starts.
  - `:checkpoint-dir`, the SparkContext's checkpoint directory.

  When it starts a local session, and `spark.serializer` isn't set, it sets
  that to `zero_one.geni.rdd.ClojureSerializer`: Spark's Java serialisation,
  which Spark uses for RDD records, except that a `false` in a record stays
  false. Java's own makes a new Boolean, which Clojure treats as true. A
  cluster's executors load their serializer before the application's jars,
  so it's left alone there.

  When the JVM lacks flags that Spark's launcher sets, it names them, in a
  warning or in the error if Spark doesn't start.

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
  (let [created? (nil? (active-session))]
    (when (and created?
               (not (#{"ALL" "TRACE" "DEBUG" "INFO"} (some-> log-level string/upper-case))))
      (quieten-spark-start!))
    (let [preset   (SparkConf.)
          master   (or master (when-not (.contains preset "spark.master") "local[*]"))
          builder  (cond-> (SparkSession/builder)
                     (or app-name (not (.contains preset "spark.app.name")))
                     (.appName (or app-name "Geni App"))

                     master
                     (.master master)

                     (and created?
                          (local-master? (or master (.get preset "spark.master")))
                          (not (.contains preset "spark.serializer"))
                          (class-named clojure-serializer))
                     (.config "spark.serializer" ^String clojure-serializer))
          builder  (reduce (fn [b [k v]] (.config b (name k) v)) builder configs)
          missing  (when created? (seq (missing-opens (spark-opens))))
          session  (try
                     (get-or-create builder)
                     (catch Throwable e
                       (if missing
                         (throw (ex-info (str "Spark didn't start: " (ex-message e) " "
                                              (missing-flags-message missing))
                                         {:missing-jvm-flags (vec missing)}
                                         e))
                         (throw e))))
          context  (if (or log-level checkpoint-dir)
                     (spark-context session)
                     (spark-context-or-nil session))]
      (when missing
        (warn! (missing-flags-message missing)))
      (cond
        log-level                                   (.setLogLevel context log-level)
        (and created? context (spark-log-profile?)) (.setLogLevel context "WARN"))
      (when checkpoint-dir
        (.setCheckpointDir context checkpoint-dir))
      session)))

(defn connect-session
  "A new session on a Spark Connect server, which becomes Spark's default and
  active session. See `g/connect`, which also makes Geni use it."
  ^SparkSession [url {:keys [configs keep-classes] :or {keep-classes true}}]
  (when-not (class-named "org.apache.spark.sql.connect.SparkSession")
    (throw (ex-info (str "Spark Connect needs Spark 4's JVM client, "
                         "org.apache.spark/spark-connect-client-jvm_2.13, on the classpath "
                         "in place of spark-sql.")
                    ;; Not the URL, which can hold a token.
                    {})))
  (let [builder (-> (SparkSession/builder) (.config "spark.api.mode" "connect"))
        builder (if url (.remote builder url) builder)
        builder (reduce (fn [b [k v]] (.config b (name k) v)) builder configs)
        ;; Not getOrCreate, which can return a session that was closed.
        session (.create builder)]
    (when-let [missing (seq (missing-opens (spark-opens)))]
      (warn! (missing-flags-message missing)))
    (when keep-classes
      ((requiring-resolve 'zero-one.geni.core.udf-artifacts/keep-classes!)))
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

(defn- version-numbers
  "The major, minor and patch numbers of a Spark version such as \"4.2.0\"."
  [^String version]
  (->> (re-find #"^(\d+)\.(\d+)(?:\.(\d+))?" version)
       rest
       (mapv #(some-> % parse-long))))

(def ^:private classpath-version*
  (delay
    (some (fn [[class-name method]]
            (some-> (class-named class-name)
                    (.getField "MODULE$")
                    (.get nil)
                    (Reflector/invokeInstanceMethod method (object-array 0))))
          [["org.apache.spark.SparkBuildInfo$" "spark_version"]
           ["org.apache.spark.package$" "SPARK_VERSION"]])))

(defn classpath-version
  "The version of the Spark on the classpath, such as \"4.2.0\": the Spark
  Connect client's, over Spark Connect. It says which of Spark's methods Geni
  can call, where the session's version says what the server runs."
  []
  @classpath-version*)

(defn require-version!
  "Throws an error that names `what` and the Spark version it needs, as
  `[major minor]` or `[major minor patch]`, when the Spark on the classpath is
  older."
  [needed what]
  (let [version (classpath-version)
        needs   (string/join "." needed)]
    (when (and version
               (neg? (compare (mapv #(or % 0) (take (count needed) (version-numbers version)))
                              (vec needed))))
      (throw (ex-info (format "%s needs Spark %s or later, and this is Spark %s." what needs version)
                      {:needs needs :spark-version version})))))

(defn- positional-args-misbound?
  "Whether the session's Spark binds more than four positional SQL parameters
  in the wrong order: 4.1.0 to 4.1.3 and 4.2.0 (SPARK-58341)."
  [^SparkSession spark n-args]
  (let [[major minor patch] (version-numbers (.version spark))]
    (and (< 4 n-args)
         (or (and (= [major minor] [4 1]) (<= (or patch 0) 3))
             (= [major minor patch] [4 2 0])))))

(defn- map-arg-error [value]
  (throw (ex-info (str "sql takes a map in its args only as a column, such as "
                       "(g/map (g/lit \"k\") (g/lit 1)), which needs Spark 4.0. Got: "
                       (pr-str value))
                  {:value value})))

(defn- sql-arg
  "A value for a SQL parameter: a column, such as a `g/lit`, as it is, a
  collection as an array literal, a keyword as its name, and anything else as
  Spark's `lit` takes it."
  [value]
  (cond
    (instance? Column value) value
    (keyword? value)         (name value)
    (map? value)             (map-arg-error value)
    (coll? value)            (functions/lit (interop/->java-array value))
    :else                    value))

(defn sql
  "Executes a SQL query using Spark, returning the result as a `DataFrame`.
  Spark runs a command, such as `CREATE TABLE`, right away, and a query when
  an action needs it.

  With `args`, the query's parameters are bound to values rather than spliced
  into the text: a map binds the named parameters, such as `:min`, and a
  vector binds the `?` ones in order. A value is a literal, a column such as
  `(g/lit ...)`, or a collection, which becomes an array: of doubles when it
  mixes whole numbers and decimals, and of arrays when it nests. From Spark
  4.0, a value can also be a column that builds an array, a map or a struct of
  literals, such as `(g/map (g/lit \"k\") (g/lit 1))`. Spark 4.1.0 to 4.1.3 and
  4.2.0 bind more than four `?` parameters in the wrong order (SPARK-58341),
  so on those `sql` throws an error for more than four, and named parameters
  work instead.

  ```clojure
  (g/sql spark \"SELECT * FROM my_table\")
  (g/sql spark \"SELECT * FROM sales WHERE price > :min\" {:min 1000})
  (g/sql spark \"SELECT ? + ?\" [2 3])
  ```"
  ([^SparkSession spark ^String sql-text]
   (. spark sql sql-text))
  ([^SparkSession spark ^String sql-text args]
   (cond
     (map? args)
     (let [named (java.util.HashMap. ^java.util.Map (update-keys (update-vals args sql-arg) name))]
       (.sql spark sql-text ^java.util.Map named))

     (and (sequential? args) (positional-args-misbound? spark (count args)))
     (throw (ex-info (str "Spark " (.version spark) " binds more than four positional parameters "
                          "in the wrong order (SPARK-58341), so use named ones, such as :a, "
                          "with a map of args.")
                     {:spark-version (.version spark) :args args}))

     (sequential? args)
     (.sql spark sql-text ^Object (object-array (map sql-arg args)))

     :else
     (throw (ex-info (str "sql takes its args as a map, for named parameters, or as a vector, "
                          "for positional ones. Got: " (pr-str args))
                     {:args args})))))

