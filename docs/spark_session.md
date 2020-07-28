# Where's The Spark Session?

> The entry point into all functionality in Spark is the [SparkSession](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html) class
> -- Spark's Official [Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html) Doc

Most Geni functions for dataset creation (including reading data from different sources) use a Spark session in the background. For instance, it is optional to pass a Spark session to the function `g/read-csv!` as the first argument. When a Spark session is not present, Geni uses the default Spark session that can be found [here](../src/zero_one/geni/defaults.clj). The default is designed to optimise for the out-of-the-box experience.

Note that the default Spark session is a delayed object that never gets instantiated unless invoked by these dataset-creation functions.

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

It is also possible to specify `:log-level` and `:checkpoint-dir`, which are set at the `SparkContext` level. By default, Spark sets the [log-level](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html) to `INFO`. In contrast, Geni sets it to `WARN` for a less verbose default REPL experience.
