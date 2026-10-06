(ns zero-one.geni.defaults
  "The SparkSession that Geni functions use when they aren't given one.
  Requiring Geni doesn't start Spark: the session is looked up, or created,
  when a function first needs it."
  (:require
   [zero-one.geni.spark :as spark])
  (:import
   (clojure.lang IDeref)
   (org.apache.spark.sql SparkSession)))

(defonce ^:private chosen (atom nil))

(defn set-default-session!
  "Makes `spark` the SparkSession that Geni functions use when they aren't
  given one, and returns it. Pass nil to go back to Spark's active session.

  ```clojure
  (g/set-default-session! (g/create-spark-session {:app-name \"My App\"}))
  ```"
  [spark]
  (when-not (or (nil? spark) (instance? SparkSession spark))
    (throw (ex-info (str "Expected a SparkSession or nil, got " (type spark))
                    {:spark spark})))
  (reset! chosen spark))

(defn- default-session ^SparkSession []
  (or @chosen
      (spark/active-session)
      (locking chosen
        (or (spark/active-session)
            (if (spark/connect-only?)
              (spark/connect-session nil {})
              (spark/create-spark-session {}))))))

(defn connect
  "Connects to a Spark Connect server, and returns a SparkSession for it:
  Spark 4's `SparkSession.builder().remote(url).create()`. It needs Spark's
  JVM client, `org.apache.spark/spark-connect-client-jvm_2.13`, on the
  classpath in place of spark-sql. See the Spark Connect guide.

  - `url`, such as \"sc://localhost:15002\", can also hold a token and other
    options, as in \"sc://host:443/;use_ssl=true;token=...\". Without it, the
    client reads the `SPARK_REMOTE` environment variable, or else connects to
    sc://localhost:15002.
  - `:configs`, a map of Spark SQL configs to set on the session.
  - `:keep-classes`, false by default. When it's true, Clojure writes the
    classes that it compiles from then on into a temporary directory, as
    `compile` does, so that a UDF of a function defined at the REPL after
    `connect` can go to the server. See `g/udf`. It's for the whole JVM:
    Clojure compiles that way in every thread, and another REPL than the one
    that calls `connect` writes classes into its own `*compile-path*`,
    \"classes\" by default, which has to exist.

  Each call starts a new session on the server, which becomes Spark's default
  and active session, and the one that Geni uses, in place of any session
  passed to `set-default-session!`. Keep it, and call `.close` on it when
  you're done.

  ```clojure
  (g/connect \"sc://localhost:15002\")
  (g/connect \"sc://localhost:15002\" {:configs {:spark.sql.shuffle.partitions 8}})
  ```"
  (^SparkSession [] (connect nil {}))
  (^SparkSession [url] (connect url {}))
  (^SparkSession [url opts]
   (let [session (spark/connect-session url opts)]
     (set-default-session! nil)
     session)))

(def spark
  "The default SparkSession, which Geni functions use when they aren't given
  one. Deref it to get the session: the one passed to `set-default-session!`,
  or else Spark's active session, or else a new local one. Geni configures
  only the sessions it creates, and then only as `create-spark-session`
  describes. With only the Spark Connect client on the classpath, the new
  session connects to `SPARK_REMOTE`, as `(connect)` does."
  (reify IDeref
    (deref [_] (default-session))))
