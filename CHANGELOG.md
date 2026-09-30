# Changelog

## Unreleased

New:

- `g/ltrim` and `g/rtrim` take the characters to trim as a second argument, as `g/trim` does, and `g/trim` trims spaces when given only a column (#344).
- `g/udf` turns a Clojure function into a Spark UDF, and `g/register-udf!` registers one for SQL and `g/expr` (#306). The function gets Clojure data, and its result is converted to the declared return type. UDFs need classic Spark. The [Clojure UDFs guide](docs/udfs.md) has the details.

Fixes:

- Functions defined at a REPL or in a script work in RDD functions, and in UDFs, on a local session that Geni starts. From a script, or from `clojure -X`, they used to fail with a `ClassCastException` about a `SerializedLambda` unless they were compiled ahead of time.
- When Geni starts Spark and there's no log4j2 config on the classpath, Spark's INFO lines as it starts no longer show: Geni sets WARN before Spark starts rather than after.
- A `false` that an RDD function closes over, directly or in a map or vector, stays false on the executors. Java's deserialisation made a new `Boolean` of it, which Clojure treats as true, so `(if b ...)` took the wrong branch.
- RDD functions defined at a REPL no longer log a warning with a stack trace for each task, about loading the `user` namespace.
- The `geni` script runs the uberjar it downloaded last time when it can't reach GitHub for the latest version, rather than stopping. Install the script again to get this.
- When Spark's Connect client isn't on the classpath, the error from `g/connect` no longer carries the URL in its `ex-data`, since the URL can hold a token.

## 0.2.0 (2026-09-30)

New:

- Spark Connect, on Spark 4. With Spark's JVM client, `org.apache.spark/spark-connect-client-jvm_2.13`, on the classpath in place of classic Spark, `g/connect` connects to a Spark Connect server, and Geni's default session connects to `SPARK_REMOTE`. The DataFrame functions work over it. The `zero-one.geni.rdd` and `zero-one.geni.ml` namespaces need classic Spark to load, and the rest of what needs it, such as `g/rdd`, the SparkContext functions and MLlib's vectors, throws an error that says so. The [Spark Connect guide](docs/spark_connect.md) has the details.

Changes:

- `g/spark-conf` returns the session's configs, `spark.conf().getAll()`, rather than the SparkContext's. That's the same settings, plus the session's SQL configs, such as `spark.sql.warehouse.dir` and any set since, and it works over Spark Connect.

## 0.1.1 (2026-09-30)

Fixes:

- The Geni CLI logs at WARN, as `spark-shell` does, from a log4j2 config in the uberjar. It used to print Spark's INFO logs as it started, and a few seconds later a warning about JDK 21's G1 Concurrent GC, over the REPL prompt. The library jar still has no log4j2 config.
- The Geni CLI no longer prints Spark's config when it starts. `(g/spark-conf @spark)` still returns it.

## 0.1.0 (2026-09-30)

Breaking changes:

- Geni now targets Spark 3.5 (on Scala 2.12 or 2.13) and Spark 4 (on Scala 2.13), on JDK 17 or 21, and the same jar works with all of them. Spark 3.4 and older, and JDK 8 and 11, are no longer supported.
- `g/nunique` keys its counts by column name, e.g. `{:SellerG 6 :Suburb 1}`, instead of Spark's `count(DISTINCT SellerG)`.
- The Leiningen template is retired. Geni itself now builds with the Clojure CLI, but you can still depend on it from Leiningen.
- Clojure is now Geni's only runtime dependency. It no longer pulls in nREPL, REPL-y, Nippy, jsonista, Potemkin, camel-snake-kebab, java.data or expound, so if your project got any of these through Geni, please add them to your own deps.
- `g/read-xlsx!` and `g/write-xlsx!` need `zero.one/fxl` on the classpath. Without it, they throw an error that says so.
- The Geni CLI (`zero-one.geni.main` and `zero-one.geni.repl`) is no longer in the library jar. It ships as the uberjar on the GitHub release, built from `cli/`.
- Requiring Geni no longer starts Spark. `zero-one.geni.defaults/spark` can still be dereffed, but it's no longer an atom: use `g/set-default-session!` rather than `reset!`.
- Geni's default session has no checkpoint directory, and no AQE configs (AQE has been on by default since Spark 3.2). Pass `:checkpoint-dir` to `g/create-spark-session` before using `g/checkpoint`, or before training ALS for many iterations, which overflows the stack without one.
- `g/create-spark-session` only sets the log level it's given with `:log-level`. Without one, it sets `WARN` only when it starts Spark and there's no log4j2 config on the classpath, as `spark-shell` does.
- ML functions such as `ml/tokenizer` throw when given a param that the Spark class has no setter for, and name the closest one, as in "Tokenizer has no param :inptu-col. Did you mean :input-col?". They used to ignore it, and some returned nil instead of the stage.

New:

- `g/set-default-session!` sets the session that Geni functions use when they aren't given one.
- Spark 4 support. Spark 4 turns on ANSI mode by default, so an invalid cast or an overflow throws rather than returning null. Geni leaves that setting to you. Spark 4 also fails a JDBC write when the table doesn't exist yet and Spark has no dialect for the database, as with SQLite, so create the table first.

Fixes:

- Geni uses a session it didn't create, such as the one on Databricks, as it is. Requiring Geni used to start a session of its own (#332), and set a checkpoint directory that Databricks rejects (#356).
- `g/create-spark-session` no longer overrides `spark.master` or `spark.app.name` when they're set already, for instance by spark-submit.
- `g/write-edn!` can write to a new path. It used to throw "already exists" unless the file existed and `:mode "overwrite"` was set.
- Without XGBoost on the classpath, `ml/xgboost-classifier`, `ml/xgboost-regressor` and `ml/write-native-model!` now throw a clear error instead of being unbound.
- `g/read-jdbc!` honours `:kebab-columns`, which it used to pass on to the JDBC source as an option, and so ignored.
- `collect-to-arrow` works on JDK 21 with Spark 3.5, as long as Arrow 13 or newer is on the classpath. Spark 3.5 ships Arrow 12, which can't allocate buffers on JDK 21.
- The `geni` script downloads the uberjar again when a new version is released, and uses curl rather than wget. It used to keep the first uberjar it downloaded, so install a script from before 0.1.0 again, or run `geni --force-download` once after each release.
