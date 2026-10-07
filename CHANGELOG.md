# Changelog

## Unreleased

Breaking changes:

- `g/median` on a column is Spark's exact `median`, rather than `percentile_approx(col, 0.5)`, so a group with an even count gets the mean of its two middle values. `g/percentile-approx` gives the approximate one, with less memory.
- `g/to-utc-timestamp`, also called `g/->utc-timestamp`, is Spark's `to_utc_timestamp`, which takes the time zone to convert from. It was `g/to-timestamp` by another name.
- `ml/summary` and `ml/binary-summary` give a summary's values as a map, such as `:accuracy`, `:area-under-roc` and `:objective-history`, with the ROC curve and the predictions as DataFrames, rather than Spark's summary object, which `(.summary model)` still gives. They take a model, or a summary for new data, as `ml/evaluate` gives with a model. A value that Spark can't give for a model is left out, such as the p-values from a solver other than the normal one, or for a fit of more features than rows.
- A whole number that fits an int goes to the functions from Spark's table as an INT literal, as it does from Spark's Scala and Python APIs, where it went as a BIGINT one. So `(g/sequence 1 5)` gives an array of INT, where it gave BIGINT. `g/lit` still makes a long a BIGINT literal, so `(g/sequence (g/lit 1) (g/lit 5))` keeps BIGINT.

New:

- Spark's SQL functions, over 300 more of them, on Spark 3.5 and 4, and over Spark Connect. Each is Spark's `functions` method of the same name in kebab case, such as `g/array-insert` for `array_insert`, and takes any of the method's overloads on the classpath. A keyword names a column, a string is a literal where Spark takes a string and a column name where it takes only a column, and any other value is a literal, so `(g/lit "x")` makes a string literal where Spark takes only a column.
  - A function that Spark 4.0, 4.1 or 4.2 added throws an error that names the version on an older Spark, and so does an arity that it added, such as `g/uuid` with a seed (Spark 4.1), and a column after the first argument of `g/split`, `g/repeat`, `g/round` or `g/bround` (Spark 4.0), which `g/split` would otherwise take as text. The functions include VARIANT's, such as `g/variant-get`, TIME's, XML's, the geospatial `g/st-*`, theta, tuple and KLL sketches, `g/listagg` and `g/string-agg`, and collations.
  - Spark's names for its aliases are there too, such as `g/ln`, `g/sign`, `g/ceiling`, `g/day`, `g/ucase`, `g/substr` and `g/nvl`, and so is `g/histogram-numeric` (#307).
  - Functions that Geni had take Spark's other arities, such as `g/round`, `g/bround`, `g/ceil` and `g/floor` with a scale, `g/split` with a limit, `g/log` with a base, `g/lag` and `g/lead` with `ignore-nulls`, and `g/sequence` without a step.
  - `g/replace-substring` is Spark's `replace`, since `g/replace` replaces values. With only a column, `g/parse-json` is Spark's `parse_json`, and with a column first, `g/count-min-sketch` is Spark's aggregate; with a DataFrame first, they're what they were. `g/reduce` folds an array column, and `g/call-function` calls a SQL function by name.
- `g/lit` takes a collection of mixed numbers, of nils, or of collections, as `g/sql` does. It widens the numbers as Clojure's arithmetic does, at any depth: to doubles with a double among them, and to decimals with a BigDecimal or a ratio. Spark gives an array of decimals the type DECIMAL(38, 18), so a decimal that it can't hold exactly throws, rather than being rounded or becoming a null.
- `g/udf` and `g/register-udf!` work over Spark Connect. Geni uploads Clojure's and Geni's jars, and the jars or source directories of the namespaces that a function uses, to each open session, once, and the server loads those namespaces from their files. `g/connect` takes `:keep-classes`, which has Clojure keep the classes that it compiles from then on, for the whole JVM, so that a function defined at the REPL after it can go to the server. A function or a file that changed after it went to a session throws an error, since the server keeps what it got first. A UDF's dates and timestamps can be java.time or java.sql values there, as on classic Spark. The [UDFs guide](docs/udfs.md) has the details.
- Results as tech.ml.dataset datasets, dtype-next tensors and Arrow, from Spark's own Arrow batches, on classic Spark and over Spark Connect. The [collecting guide](docs/collect.md) has the details.
  - `g/to-tmd` collects a result as one tech.ml.dataset dataset, with a column per Spark column that keeps its type: DATE as LocalDates, TIMESTAMP as Instants, TIMESTAMP_NTZ as LocalDateTimes, day-time intervals as Durations of any length, year-month intervals as Periods, arrays as vectors, structs and maps as maps, and nulls as missing values. Each column keeps its Spark type, as DDL, in its metadata, unless it holds MLlib vectors. A calendar interval, a geometry, a geography or a struct with two fields of one name throws, naming its column, as do two columns that `:key-fn` names alike, before a job runs, and rows without columns. It needs `techascent/tech.ml.dataset` on the classpath.
  - `g/stream` reads a result as a dataset per Arrow batch, as a reducible that stops reading when a reduce is done, stops early or throws. On classic Spark, each partition runs as a job of its own when the reduce gets to it. It's seqable too, and a seq reads a batch at a time.
  - `g/to-tensors` and `g/stream-tensors` turn columns of integers or floating-point numbers, and columns of arrays or dense MLlib vectors of them of one length, into dtype-next tensors.
  - `g/to-arrow` collects a result as Arrow IPC streams in memory, one per batch.
  - Over Spark Connect, all but `g/to-arrow` need `org.apache.arrow/arrow-vector` and `arrow-memory-netty` on the classpath, since the client only has Arrow shaded.
  - On classic Spark, `g/to-tmd`, `g/to-tensors`, `g/to-arrow` and a reduce over `g/stream` or `g/stream-tensors` run as one of Spark's SQL executions, as `g/collect` does, so an observation from `g/observe` gets its metrics, and Spark's UI lists the query.
- `g/create-dataframe` takes a tech.ml.dataset dataset. Each column's Spark type comes from `:schema`, from the type that `g/to-tmd` kept in its metadata, so a round trip keeps the types, or from its datatype or values. A DECIMAL has room for every value in its column, and takes a float or a double as its shortest decimal, as Spark reads one. A value that its column's type can't hold exactly throws, naming the column, rather than being rounded or becoming a null.
- `g/glimpse` prints a column per line, with its type and its first values, and `g/to-html` gives Spark's HTML table of a DataFrame's first rows, as a notebook shows it.
- `g/records->dataset`, `g/map->dataset` and `g/table->dataset` infer a day-time interval for a `java.time.Duration` and a year-month interval for a `java.time.Period`, and on Spark 4, VARIANT for Spark's `VariantVal` and, from Spark 4.1, TIME for a `java.time.LocalTime`.
- More of Spark's Dataset API, on Spark 3.5 and 4, and over Spark Connect:
  - `g/offset` skips rows, and `g/unpivot`, also called `g/melt`, turns columns into rows.
  - `g/with-columns` adds or replaces columns from a map, and adds the new ones in the map's order.
  - `g/to` reconciles a DataFrame with a schema, as a struct type, Geni's schema data or a DDL string: it puts the columns in the schema's order, casts them where that's safe, drops the ones the schema lacks and fills a missing nullable one with nulls.
  - `g/with-metadata` sets a column's metadata from a map, and `g/column-metadata` reads it back.
  - `g/observe` computes aggregates while an action runs, and `g/observed` returns them from a `g/observation`.
  - `g/metadata-column` selects a metadata column, such as the `_metadata` column of a file source.
  - `g/ilike` matches a pattern without regard to case, and `g/with-field` and `g/drop-fields` set and drop the fields of a struct column.
  - `g/sample` takes a seed, `g/union-by-name` takes `{:allow-missing-columns true}` after the DataFrames, and `g/print-schema` takes a depth, as does `g/tree-string`, which returns the tree.
  - `g/explain` takes a mode, such as `:formatted` or `:cost`, and `g/explain-string` returns the plan. `g/same-semantics` and `g/semantic-hash` compare plans, and `g/parse-ddl` turns a DDL string into a Spark type.
- Reading, writing and SQL as Spark's own APIs have them, on Spark 3.5 and 4, and over Spark Connect:
  - `g/read!` and `g/write!` read and write any data source, such as Delta Lake, with a `:format`, a `:path` or `:paths`, and the source's own options. Without a path, they read and write what the options name, as a JDBC source does.
  - `g/write-table!` takes `:format`, and `:bucket-by` and `:sort-by` for bucketed tables, and `g/read-table!` takes reader options.
  - `g/insert-into!` inserts a DataFrame's rows into an existing table, by position.
  - `g/write-to!` writes through Spark's DataFrameWriterV2, for catalogs such as Delta's and Iceberg's, with a `:mode` such as `:create`, `:append` or `:overwrite`. Spark's built-in session catalog only takes `:create`.
  - `g/parse-json` and `g/parse-csv` parse a column of JSON or CSV strings into a DataFrame.
  - `g/sql` binds parameters to values: a map binds named ones, such as `:min`, and a vector binds `?` ones. A collection becomes an array, as `g/lit` makes one.
  - `g/conf-get`, `g/conf-set!`, `g/conf-unset!` and `g/conf-modifiable?` read and set the session's runtime configs.
  - `zero-one.geni.catalog` has `list-catalogs`, `current-catalog` and `set-current-catalog`.
  - The writers' `:mode` can be a keyword, such as `:overwrite`, as well as a string.
- Checkpoints that free what they hold, on Spark 3.5 and 4, and over Spark Connect:
  - `g/local-checkpoint` cuts a Dataset's plan with a checkpoint in the executors' storage, which needs no checkpoint directory, and from Spark 4.0 takes the storage level.
  - `g/release-checkpoint!` frees a checkpoint: the blocks of a local one, and the files of a reliable one. Over Spark Connect, the server lets go of it, for its context cleaner to free.
  - `g/with-checkpoint` binds checkpointed Datasets, as `with-open` does, and releases them when its body is done.
- Spark 4's verbs, which on an older Spark throw an error that names the version they need:
  - `g/transpose`, `g/grouping-sets`, and `g/lateral-join`, with `g/outer` for the left side's columns (Spark 4.0).
  - Subqueries: `g/scalar`, and `g/exists` with a DataFrame (Spark 4.0), and `g/isin` with a DataFrame (Spark 4.1).
  - `g/try-cast`, which gives null where a value doesn't convert (Spark 4.0).
  - `g/zip-with-index` and `g/nearest-by-join` (Spark 4.2).
  - `:cluster-by` for `g/write-table!` and `g/write-to!` (Spark 4.0).
  - `g/read-changes!`, which reads a table's change feed, from a catalog that has one, such as Delta Lake's (Spark 4.2).
- `g/table-function` calls a table-valued function, such as `:explode`, `:inline` or `:stack`, on any Spark.
- More of Spark ML, on classic Spark:
  - `ml/r-formula`, `ml/univariate-feature-selector`, `ml/variance-threshold-selector`, `ml/vector-slicer`, and `ml/target-encoder`, which needs Spark 4.0.
  - `ml/string-indexer-model` and `ml/count-vectorizer-model` make those models from known labels or a known vocabulary, without fitting. `ml/load-default-stop-words` gives Spark's stop words for a language, and `ml/array-to-vector` turns arrays into vectors.
  - Predictions for one row's features, as numbers, an MLlib vector or a sparse vector's map, as `g/collect` gives one: `ml/predict`, `ml/predict-raw`, `ml/predict-probability`, `ml/predict-leaf` and `ml/predict-quantiles`.
  - More of the models' attributes: `ml/selected-features`, `ml/resolved-formula-string`, `ml/to-debug-string`, `ml/evaluate-each-iteration`, `ml/explained-variance`, `ml/doc-freq`, `ml/num-docs`, `ml/find-synonyms` for a word or a vector, `ml/get-vectors`, LDA's `ml/topics-matrix`, `ml/log-prior`, `ml/training-log-likelihood`, `ml/to-local` and `ml/get-checkpoint-files`, RobustScaler's `ml/median` and `ml/range`, `ml/sigma`, `ml/factors`, `ml/linear`, `ml/compute-cost`, ALS's `ml/rank`, `ml/get-splits`, `ml/get-splits-array`, `ml/labels-array` and `ml/has-summary?`.
  - `ml/evaluate` with a model in place of an evaluator gives the model's summary for new data.
  - `ml/avg-metrics`, `ml/validation-metrics` and `ml/sub-models` give a tuned model's metric for each param map, and the models fitted for them. `ml/cross-validator` and `ml/train-validation-split` take `:collect-sub-models`, and `ml/train-validation-split` takes `:train-ratio`.
  - `ml/summarizer` aggregates a vector column's statistics, such as the mean and variance of each feature, `ml/correlation` gives a vector column's correlation matrix by Pearson's or Spearman's method, and `ml/chi-square-test` takes `flatten`, for a row per feature.
  - `ml/assign-clusters` runs power iteration clustering, which assigns clusters rather than fitting a model, and `ml/larger-better?` says whether an evaluator's larger metric is the better one.

Fixes:

- `zero-one.geni.core.column/not-equal`, which `g/` doesn't export, was null-safe equality, `g/<=>`, rather than `g/=!=`.
- On Spark 4.1.0 to 4.1.3 and 4.2.0, which bind more than four positional SQL parameters in the wrong order (SPARK-58341), `g/sql` throws an error that says so for more than four, rather than returning wrong results, as it does for a vendor's version without a patch number, such as "4.2". Named parameters work.
- A reader or writer option with a string key, such as Iceberg's `"snapshot-id"`, goes to Spark as it is. It was turned into camelCase, as a keyword key is, so an option with a hyphen or an underscore in its name was lost. A keyword value, such as `:failfast` for `:mode`, goes as its name.
- `g/read-table!` and `g/write-table!` take a keyword as the table's name.
- `g/write-csv!` leaves out the header row when its options say `:header false`. It always wrote one.
- On JDK 25, Spark 4.2's sketch functions, such as `g/hll-sketch-agg`, threw "Unsupported JDK Major Version" with the README's Spark 4 setup. That setup put datasketches-memory ahead of spark-catalyst on the classpath. Spark 4.2 ships its own copy of datasketches' JDK check, which takes JDK 25, so the Spark 4 setups in the README and the template now list `spark-catalyst` to put Spark's copy first.
- `ml/coefficients`, `ml/coefficient-matrix` and the other model functions that give a vector's values gave only the stored values of a sparse vector, so a logistic regression's coefficients could come back as 414 numbers for 780 features. They give every value now.
- `g/posexplode-outer` was `g/posexplode`, which drops the rows whose array or map is null or empty, and `zero-one.geni.core.functions/explode-outer` was `explode`. They're Spark's outer ones now, and `g/explode-outer` is in `g/`.
- `g/records->dataset`, `g/map->dataset` and `g/table->dataset` give a column of decimals room for all its values, at the top or in arrays and structs: `DecimalType(38,18)` for BigDecimals and `DecimalType(38,0)` for whole numbers as before, when the values fit, and otherwise as many digits after the point as the values need, with 38 in all. A value that didn't fit became a null on Spark 3.5, and on Spark 4 threw or was rounded. A column that no DECIMAL holds throws an error that names it.
- When a namespace that an RDD function or a UDF uses has a file that fails to load on an executor, the task fails with that error. It was logged as a warning, and the function failed later, on a var that wasn't bound.
- The namespaces that the executors load for an RDD function or a UDF include those of the records and types in what it closes over, whose classes they make. A cluster's executors couldn't read such a function without them.

## 0.3.0 (2026-10-01)

Breaking changes:

- The XGBoost wrappers need XGBoost4J-Spark 3, tested with 3.4.0, whose one jar takes the place of 1.x's `xgboost4j-spark` and `xgboost4j`. They take XGBoost's own defaults rather than the ones Geni set from XGBoost4J 1.2, such as 100 rounds rather than 1, 256 bins rather than 16, and NaN rather than 0.0 for a missing value, so pass `:num-round` and the like to keep a model as it was. 3.4.0, the latest on Maven Central, predicts wrongly on sparse feature vectors, such as LIBSVM data's, and needs `:missing` to fit on them, so turn them into arrays with `ml/vector-to-array` first, as the XGBoost guide shows. On Spark 4, an XGBoost model doesn't save as a Spark ML stage, since 3.4.0 is built against Spark 3.5's json4s, but `ml/write-native-model!` works. A param that XGBoost 3 dropped, such as `:cache-training-set` or `:rabit-timeout`, throws, with the params there are.

New:

- `ml/xgboost-ranker`, XGBoost4J-Spark 3's learning-to-rank estimator, which takes a `:group-col`.
- A deps-new template for a new project, with the Spark setups, a small app and its test, and an uberjar for `spark-submit`. The README's "A New Project" has the command.
- `g/ltrim` and `g/rtrim` take the characters to trim as a second argument, as `g/trim` does, and `g/trim` trims spaces when given only a column (#344).
- When the JVM lacks `--add-opens` flags that Spark's launcher sets, which Spark needs on JDK 17 and later, Geni names them as it starts Spark or connects to a Spark Connect server: in the error when Spark doesn't start, as Spark 3.5 doesn't without `sun.nio.ch`, and in a warning otherwise.
- `g/udf` turns a Clojure function into a Spark UDF, and `g/register-udf!` registers one for SQL and `g/expr` (#306). The function gets Clojure data, and its result is converted to the declared return type. UDFs need classic Spark. The [Clojure UDFs guide](docs/udfs.md) has the details.

Fixes:

- Functions defined at a REPL or in a script work in RDD functions, and in UDFs, on a local session that Geni starts. From a script, or from `clojure -X`, they used to fail with a `ClassCastException` about a `SerializedLambda` unless they were compiled ahead of time.
- When Geni starts Spark and there's no log4j2 config on the classpath, Spark's INFO lines as it starts no longer show: Geni sets WARN before Spark starts rather than after.
- A `false` that an RDD function closes over, directly or in a map or vector, stays false on the executors. Java's deserialisation made a new `Boolean` of it, which Clojure treats as true, so `(if b ...)` took the wrong branch.
- RDD functions defined at a REPL no longer log a warning with a stack trace for each task, about loading the `user` namespace.
- The namespaces that the executors load for an RDD function or a UDF now include those of the vars in the collections it closes over, such as a vector of functions, and the namespace of a record whose method made the function. A cluster's executors could miss them before.
- An ML param with overloaded setters gets the one that suits its value, as XGBoost's `:features-col` does, which takes a column or several.
- The `geni` script runs the uberjar it downloaded last time when it can't reach GitHub for the latest version, rather than stopping. Install the script again to get this.
- When Spark's Connect client isn't on the classpath, the error from `g/connect` no longer carries the URL in its `ex-data`, since the URL can hold a token.
- `rdd/collect`, `rdd/take` and the other RDD actions hand back Clojure maps, vectors and sets as they were, with what's in them converted. They used to turn them into seqs, so `{:ok false}` came back as `(((:ok false)))`.
- A `false` in an RDD record stays false, on the executors and when it's collected, on a local session that Geni starts. Spark's Java serialisation, which Spark uses for RDD records, made a new `Boolean` of it, which Clojure treats as true. Geni sets `spark.serializer` to `zero_one.geni.rdd.ClojureSerializer`, which reads booleans back as `true` and `false`, unless `spark.serializer` is set already. A cluster's executors load their serializer before they fetch the application's jars, so Geni doesn't set it there; set it yourself when Geni's jar is on the executors' own classpath, as with `spark.executor.extraClassPath`. Either way, the RDD actions hand back `true` and `false` on the driver.
- `g/records->dataset`, `g/map->dataset` and `g/table->dataset` infer a type for decimals, dates and times, keywords and UUIDs, which failed with a `ClassCastException`: `DecimalType(38,18)` for a `BigDecimal`, `DecimalType(38,0)` for a `BigInt`, `DateType` for a `LocalDate`, `TimestampType` for an `Instant`, `TimestampNTZType` for a `LocalDateTime`, and `StringType` for a keyword or a UUID. A `java.util.Date`, such as `#inst`, becomes a timestamp, where it used to be a date that Spark rejected. A value of another class throws an error that names its column. The [manual dataset creation guide](docs/manual_dataset_creation.md) lists the types.

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
