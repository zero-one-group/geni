# Contributing to Geni

First and foremost, thank you for taking an interest in contributing to Geni! All kinds of contributions are welcome: bug reports, feature requests, docs, examples and code.

If you'd like to report an issue or request a feature, please [create an issue](https://github.com/zero-one-group/geni/issues). If you'd like to have a crack at one, please leave a comment on the issue first, so that we don't duplicate the work.

## Setting up

You'll need JDK 21 (17 works too) and the [Clojure CLI](https://clojure.org/guides/install_clojure). Then, from the repo root:

```bash
clojure -T:build prep
```

This compiles the Java sources, plus the namespaces that the RDD tests need ahead of time. Run it again whenever you change anything under `src/java`, or switch to another Spark alias (see below).

## Running the tests

The tests use `clojure.test`, with a small runner in `dev/`:

```bash
clojure -X:spark:test                                        # the main suite
clojure -X:spark:test :exclude :slow                         # skip the slow tests
clojure -X:spark:test :only '[zero-one.geni.dataset-test]'   # one namespace
clojure -X:spark:test:cli:tmd                                # what the CI runs
clojure -X:spark:test:xgb                                    # plus the XGBoost tests
```

Each namespace gets one line, and each failure one more. The full reports, with stack traces, are in `target/test.log`. The `:cli`, `:tmd` and `:xgb` aliases add the tests for the Geni CLI, tech.ml.dataset and XGBoost, along with their deps. The XGBoost tests don't train on Apple Silicon yet, and only run with `:spark`.

`:spark` is Spark 3.5 on Scala 2.12. To run the tests on Spark 3.5 on Scala 2.13, or on Spark 4, swap in `:spark-3.5-2.13` or `:spark-4`, after a `prep` for the same alias:

```bash
clojure -T:build prep :spark :spark-4
clojure -X:spark-4:test
```

The runner says so if `prep` was for another Spark.

From a REPL started with `clj -M:spark:test`, this reloads a test namespace and runs it:

```clojure
(require '[zero-one.geni.test-runner :refer [test!]])
(test! 'zero-one.geni.dataset-test)
```

The tests run with Spark's code generation off (see the `:test` alias), which makes their many small queries faster. Adding `:canary` after `:test` turns it back on, which is how the weekly canary runs them, on the newest Spark patches.

On a pull request, the CI runs `:spark` and `:spark-4` on JDK 21, split into two shards with `:shard '[1 2]'` and `:shard '[2 2]'`, plus `:spark` on arm64. A push to `develop` also covers JDK 17 and `:spark-3.5-2.13`.

## Linting and formatting

```bash
clojure -M:kondo --lint src test/zero_one cli test-tmd test-xgb dev build.clj
clojure -M:fmt check src test cli test-tmd test-xgb docs dev build.clj   # or `fix` instead of `check`
```

The CI runs the same commands on every pull request, so it's worth running them before you push.

## Pull requests

Please fork the repo, branch off `develop` and open a pull request against `develop`. Much of the actual data work is done by Spark, so the tests mostly need to check that we're calling the right Spark functions and methods.

## Releasing

Set `version` in `build.clj` (a pre-release has a suffix, like `0.1.0-alpha.1`) and get it onto `develop`. Then tag that commit and push the tag:

```bash
git tag v0.1.0-alpha.1
git push origin v0.1.0-alpha.1
```

The Release workflow deploys `zero.one/geni` to Clojars, builds the CLI uberjar and attaches it to a GitHub release, marked as a pre-release when the version has a suffix. It skips the deploy when Clojars already has that version, so a failed run can be rerun, or the tag moved to a fixed commit and pushed again. After a final release, bump `resources/GENI_REPL_RELEASED_VERSION` on `develop`, so that installed `geni` scripts pick the new uberjar up.

The same build tasks run locally: `clojure -T:build deploy`, with `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` set, and `clojure -T:build cli-uber` for the uberjar.
