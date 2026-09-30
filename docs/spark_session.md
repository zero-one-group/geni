# Where's The Spark Session?

> "The entry point into all functionality in Spark is the [SparkSession](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html) class."  
[Spark's Official Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html)

Most Geni functions that create datasets, such as `g/read-csv!` or `g/range`, take a Spark session as an optional first argument. Without one, they use Geni's [default session](../src/clojure/zero_one/geni/defaults.clj), which is:

1. the session passed to `g/set-default-session!`, if there is one;
2. otherwise Spark's active session, for instance the one that Databricks or your own code started;
3. otherwise a new local session, which Geni creates the first time a function needs one. With Spark's [Spark Connect](spark_connect.md) client on the classpath in place of Spark, it connects to the server at `SPARK_REMOTE` instead.

Requiring Geni doesn't start Spark, and Geni leaves the settings of a session it didn't create alone.

## Creating A Spark Session

The following Scala Spark code:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .master("local")
  .appName("Basic Spark App")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
```

translates to:

```clojure
(require '[zero-one.geni.core :as g])

(g/create-spark-session
  {:master   "local"
   :app-name "Basic Spark App"
   :configs  {:spark.some.config.option "some-value"}})
```

Like `getOrCreate`, it returns the running session if there is one. Without `:master` and `:app-name`, it uses `"local[*]"` and `"Geni App"`, unless `spark.master` and `spark.app.name` are set already, for instance by `spark-submit`.

It also takes `:log-level` and `:checkpoint-dir`, which are set on the `SparkContext`. Without a log4j2 config of its own, an app gets Spark's default profile, which logs at `INFO`. So when Geni starts Spark and there's no log4j2 config on the classpath, it sets the level to `WARN`, as `spark-shell` does. A log4j2 config or `:log-level` takes precedence.

Geni's default session has no checkpoint directory, so `g/checkpoint` needs one first:

```clojure
(g/create-spark-session {:checkpoint-dir "target/checkpoint"})
```

## Using Another Session

A session created with `g/create-spark-session`, or with Spark's builder, becomes Spark's active session, and Geni picks it up. For a session that isn't the active one, such as one from `.newSession`, use `g/set-default-session!`:

```clojure
(def isolated (.newSession (g/create-spark-session {})))

(g/set-default-session! isolated) ; Geni functions now use `isolated`
(g/set-default-session! nil)      ; back to Spark's active session
```
