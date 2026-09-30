# Spark Connect

[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) splits Spark in two: a server runs the driver, and a thin client sends it query plans over gRPC and gets the results back as Arrow batches. On Spark 4, Geni works as such a client. The same functions build the queries, and the server runs them. It's also the way to [Databricks](#databricks) from outside a notebook, serverless compute included.

<!-- #:test-doc-blocks{:meta :connect :apply :all-next} -->

## Setting up

Spark's JVM client takes the place of Spark in your deps. It can't share a classpath with `spark-sql`: it carries its own copy of Spark's SQL API, with Arrow shaded inside, and stand-ins for classes such as `SparkContext`. It needs Spark 4's JVM flags, since Arrow and Spark's date and time conversions reach into the JDK. This alias is the one that Geni's own Spark Connect tests use:

```edn
{:aliases
 {:spark-connect
  {:extra-deps {org.apache.spark/spark-connect-client-jvm_2.13 {:mvn/version "4.2.0"}}
   :jvm-opts   ["-XX:+IgnoreUnrecognizedVMOptions"
                "--add-modules=jdk.incubator.vector"
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
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
                "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                "-Dio.netty.tryReflectionSetAccessible=true"
                "-Dio.netty.allocator.type=pooled"
                "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"
                "--sun-misc-unsafe-memory-access=allow"
                "--enable-native-access=ALL-UNNAMED"]}}}
```

Use the client from the same Spark release as the server. A Spark 4 distribution starts a server with `sbin/start-connect-server.sh`, which listens on port 15002, and [Spark's guide](https://spark.apache.org/docs/latest/spark-connect-overview.html) covers the other ways.

## Connecting

```clojure
(require '[zero-one.geni.core :as g])

(def spark (g/connect "sc://localhost:15002"))
```

`g/connect` starts a session on the server, and makes it the default session, so Geni's functions use it without being given it, in place of any session passed to `g/set-default-session!`. The URL can also carry a token and other options, as in `"sc://host:443/;use_ssl=true;token=..."`. Without a URL, `g/connect` reads the `SPARK_REMOTE` environment variable. So does Geni's default session, when the client is on the classpath and no session has been started.

## Queries

The DataFrame functions work as they do on classic Spark. The server reads the files, so a relative path is relative to where the server runs, which here is Geni's repo:

```clojure
(def housing (g/read-parquet! spark "test/resources/housing.parquet"))

(-> housing
    (g/group-by :ocean_proximity)
    (g/agg {:count      (g/count "*")
            :mean-value (g/mean :median_house_value)})
    (g/order-by (g/desc :count))
    g/show)
;; =stdout=>
; +---------------+-----+------------------+
; |ocean_proximity|count|mean-value        |
; +---------------+-----+------------------+
; |INLAND         |1823 |102618.0493691717 |
; |<1H OCEAN      |1783 |237567.07683679194|
; |NEAR BAY       |1287 |205933.50271950272|
; |NEAR OCEAN     |107  |90349.53271028037 |
; +---------------+-----+------------------+
```

Data from the client goes to the server with the query:

```clojure
(-> (g/records->dataset [{:item "tea" :price 3.5}
                         {:item "cake" :price 12.0}
                         {:item "coffee" :price 4.0}])
    (g/filter (g/< :price 5))
    (g/order-by :item)
    g/collect)
;; => ({:item "coffee", :price 4.0} {:item "tea", :price 3.5})
```

Spark Connect analyses a query when it needs its schema or its rows, rather than when it's built. So a typo in a column name shows up at `g/collect`, not at `g/select`.

## What needs classic Spark

A Spark Connect session has no `SparkContext`, and the client has no MLlib. So these need classic Spark:

- RDDs: `g/rdd`, `g/partitions`, and the `zero-one.geni.rdd` namespace, which doesn't load without classic Spark;
- the SparkContext functions, such as `g/java-spark-context`, `g/app-name` and `g/default-parallelism`;
- MLlib: the `zero-one.geni.ml` namespace, which doesn't load without classic Spark either, and MLlib's vectors, such as `g/dense`, `g/sparse`, `g/corr` on a vector column, and LIBSVM's features;
- [Clojure UDFs](udfs.md), `g/udf` and `g/register-udf!`, since the server can't load Clojure functions;
- `g/sample-by` with a struct column, since the client can't send a struct as a literal.

The functions throw an error that says so:

```clojure
(try
  (g/app-name spark)
  (catch clojure.lang.ExceptionInfo e
    (ex-message e)))
;; => "This needs a classic SparkSession, with a SparkContext. A Spark Connect session has none, so RDDs, broadcasts and MLlib don't work over it."
```

`g/collect-to-arrow` needs Arrow's own jars, `org.apache.arrow/arrow-vector` and `arrow-memory-netty`, since the client only has Arrow shaded.

When you're done, `.close` releases the session on the server:

```clojure
(.close spark)
```

## Databricks

Databricks Connect is Databricks' build of the Spark Connect client, and the one that reaches serverless compute. It goes in place of Spark's client, as `com.databricks/databricks-connect_2.13`, at the version that matches the Databricks Runtime: 19.x for Runtime 19, which is on Spark 4.2. Databricks Connect 18 and 19 need JDK 21. `DatabricksSession` starts a session from the usual Databricks settings, such as the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables or a config profile, and Geni uses it as it does any other:

<!-- :test-doc-blocks/skip -->
```clojure
(import '(com.databricks.connect DatabricksSession))

(g/set-default-session! (.getOrCreate (DatabricksSession/builder)))

;; or, on serverless compute:
(g/set-default-session! (-> (DatabricksSession/builder) .serverless .getOrCreate))
```

Geni's tests don't cover Databricks, so reports of what works there and what doesn't are welcome on GitHub.
