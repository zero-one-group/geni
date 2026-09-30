# {{name}}

{{description}}

It runs on Spark 3.5 with the `:spark` alias, or on Spark 4 with `:spark-4` in its place. Spark needs JDK 17 or 21.

```bash
clojure -M:spark:run                   # the word counts of two lines of its own
clojure -M:spark:run path/to/file.txt  # the word counts of a text file
clojure -X:spark:test                  # the tests
```

For a cluster, `clojure -T:build uber` builds `target/{{main/file}}.jar`, with the app and Geni compiled ahead of time, and Clojure, but not Spark, which `spark-submit` provides. Use `clojure -T:build uber :spark :spark-4` for Spark 4. The jar names its main class, so `spark-submit` needs no `--class`:

```bash
spark-submit target/{{main/file}}.jar path/to/file.txt
```

Geni's [README](https://github.com/zero-one-group/geni) and guides have the rest.
