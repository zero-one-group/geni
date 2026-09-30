# Changelog

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
