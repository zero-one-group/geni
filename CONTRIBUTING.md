# Contributing to Geni

First and foremost, thank you for taking an interest in contributing to Geni! All kinds of contributions are welcome: bug reports, feature requests, docs, examples and code.

If you'd like to report an issue or request a feature, please [create an issue](https://github.com/zero-one-group/geni/issues). If you'd like to have a crack at one, please leave a comment on the issue first, so that we don't duplicate the work.

## Setting up

You'll need JDK 21 (17 works too) and the [Clojure CLI](https://clojure.org/guides/install_clojure). Then, from the repo root:

```bash
clojure -T:build prep
```

This compiles the Java sources, plus the namespaces that the RDD tests need ahead of time. Run it again whenever you change anything under `src/java`.

## Running the tests

```bash
clojure -X:spark:test                                        # the main suite
clojure -X:spark:test :exclude :slow                         # skip the slow facts
clojure -X:spark:test :only '[zero-one.geni.dataset-test]'   # one namespace
clojure -X:spark:test:cli:tmd                                # what the CI runs
clojure -X:spark:test:xgb                                    # plus the XGBoost tests
```

The `:cli`, `:tmd` and `:xgb` aliases add the tests for the Geni CLI, tech.ml.dataset and XGBoost, along with their deps. The XGBoost tests don't train on Apple Silicon yet. For a REPL with the test paths on the classpath, run `clj -M:spark:test`.

## Linting and formatting

```bash
clojure -M:kondo --lint src test/zero_one cli test-tmd test-xgb dev build.clj
clojure -M:fmt check src test cli test-tmd test-xgb docs dev build.clj   # or `fix` instead of `check`
```

The CI runs the same commands on every pull request, so it's worth running them before you push.

## Pull requests

Please fork the repo, branch off `develop` and open a pull request against `develop`. Much of the actual data work is done by Spark, so the tests mostly need to check that we're calling the right Spark functions and methods.

## Releasing

Bump `version` in `build.clj`, then run `clojure -T:build deploy` with `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` set.
