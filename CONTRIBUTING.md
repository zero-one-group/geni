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
clojure -T:build xgb-tests                                   # the XGBoost tests and guide
```

Each namespace gets one line, and each failure one more. The full reports, with stack traces, are in `target/test.log`. The `:cli` and `:tmd` aliases add the tests for the Geni CLI and tech.ml.dataset, along with their deps.

`xgb-tests` runs the tests in `test-xgb/` and the XGBoost guide's examples, with XGBoost4J-Spark on the classpath: the `:xgb` alias on `:spark`, and `:xgb-2.13` on the Spark builds on Scala 2.13, as in `clojure -T:build xgb-tests :spark :spark-4` after the matching `prep`. XGBoost's native library needs OpenMP, which is `brew install libomp` on macOS. Every other run skips the tests marked `^:xgb`, and a run on Spark 4 skips the ones marked `^:spark-3`, such as the guide's Spark ML stage example.

`:spark` is Spark 3.5 on Scala 2.12. To run the tests on Spark 3.5 on Scala 2.13, or on Spark 4, swap in `:spark-3.5-2.13` or `:spark-4`, after a `prep` for the same alias:

```bash
clojure -T:build prep :spark :spark-4
clojure -X:spark-4:test
```

The runner says so if `prep` was for another Spark.

`clojure -T:build connect-tests` runs the suite over Spark Connect. It starts a Spark Connect server on `:spark-4` in the background, with its log in `target/connect-server.log`, then runs the tests with Spark's JVM client in place of classic Spark (the `:spark-connect` alias). That run skips the tests marked `^:classic`, such as the RDD and MLlib ones, and every other run skips the ones marked `^:connect`. Then it runs the tech.ml.dataset tests in `test-tmd/`, with Apache Arrow's own jars added (the `:tmd` and `:connect-arrow` aliases), and the examples in the Spark Connect guide, which are marked `^:connect` too, and connect to port 15002. Other options go to the runner, and they skip `test-tmd/` and the guide, as another `:port` does:

```bash
clojure -T:build prep :spark :spark-4
clojure -T:build connect-tests
clojure -T:build connect-tests :only '[zero-one.geni.dataset-test]'
```

From a REPL started with `clj -M:spark:test`, this reloads a test namespace and runs it:

```clojure
(require '[zero-one.geni.test-runner :refer [test!]])
(test! 'zero-one.geni.dataset-test)
```

The tests run with Spark's code generation off (see the `:test` alias), which makes their many small queries faster. Adding `:canary` after `:test` turns it back on, which is how the weekly canary runs them, on the newest Spark patches.

On a pull request, the CI runs `:spark` and `:spark-4` on JDK 21, split into two shards with `:shard '[1 2]'` and `:shard '[2 2]'`, plus `:spark` on arm64, the suite over Spark Connect, and `xgb-tests` on `:spark` (x64 and arm64) and `:spark-4`. A push to `develop` also covers JDK 17 and `:spark-3.5-2.13`. Each run also installs the library jar and uses it from the README's `deps.edn`, on Clojure 1.11.4 and JDK 17, the oldest that Geni supports.

## Spark's functions

Most of Spark's SQL functions are rows of the table at the end of `src/clojure/zero_one/geni/core/functions.clj`: a name, its argument lists, and `:since` for one that Spark 4.0 or later added. A new one also needs an example in `test/zero_one/geni/spark_functions_test.clj`, which runs each function against the same call in SQL, and its docstring in `resources/spark-function-docs.edn`, which `zero-one.geni.function-docs` writes from Spark's `functions.scala` at the tag that `:spark-4` pins:

```bash
clojure -X:spark:test zero-one.geni.function-docs/write! :scala '"../spark/sql/api/src/main/scala/org/apache/spark/sql/functions.scala"'
```

The same test file's `coverage-test` prints, to stderr, the functions of the Spark on the classpath that Geni doesn't have yet, so the weekly canary shows the ones that a newer Spark adds.

## The project template

`template/` is the deps-new template that the README shows, with its files under `template/resources/zero_one/geni/`. `template_test` checks its Spark setups against `deps.edn` and its `:geni/version` against `build.clj`, so a release bumps that too. The `Template` workflow makes a project from it when it changes, runs the project's test, and runs its uberjar with `spark-submit`.

## Linting and formatting

```bash
clojure -T:build lint    # clj-kondo, then cljfmt's check
clojure -T:build fmt     # reformats the sources with cljfmt
clojure -T:build check       # lint, the tests on :spark, and the doc tests
clojure -T:build check-all   # check, plus the tests on :spark-4 and over Spark Connect
```

`check` is what the CI runs on a pull request, so running it before you push saves a round trip. `check-all` adds the tests on `:spark-4` and `connect-tests`, preps each Spark itself, and ends prepped for `:spark`. It stops at the first failure, and takes several minutes. Both `lint` and `check` call `clojure -M:kondo` and `clojure -M:fmt` underneath, which take the same arguments as the tools themselves.

## Executable documentation

After `prep`, run the README and reference examples with `clojure -T:build docs`.
`clojure -T:build cookbook` runs the cookbook separately. The first run downloads
its public datasets into `data/cookbook/` and generates part 10's 12 million rows
there, about 500 MB in all, and takes a few minutes. Part 5 uses the weather data
written by part 4.
The weekly workflow runs the cookbook and checks Markdown links.

`test-doc-blocks` turns Clojure fences into tests, in document order. Put
`;; =>` before a returned value or `;; =stdout=>` before commented printed output.
Ordinary comments are illustrative and are not assertions. A block without an
expectation still runs, so errors in setup and examples fail the build. For a
block that requires external infrastructure, add `<!-- :test-doc-blocks/skip -->`
immediately before its fence and explain why it is skipped.

The generated assertions use helpers in `dev/zero_one/geni/doc_assertions.clj`:
floating-point values allow `1e-6` relative or `1e-9` absolute error, including
decimal numbers in printed output. Spark table padding may vary with those
numbers. Structure, keys, integer values, text and row order are still checked;
the library's regular tests keep exact equality. Doc runs use two local Spark
cores, code generation and UTC. Use fixed seeds and explicit ordering when an
example depends on them.

To refresh existing expectations after changing an example:

```bash
clojure -X:spark:test:doc-tests zero-one.geni.doc-outputs/regen :files '["README.md" "docs/examples.md"]'
```

This executes the examples, including their file writes, and updates their
expectation comments. Review the diff and run `docs` or `cookbook` afterward.
Values that cannot round-trip through the Clojure reader and output with trailing
spaces remain illustrative comments; the refresher reports these cases.

## Pull requests

Please fork the repo, branch off `develop` and open a pull request against `develop`. Much of the actual data work is done by Spark, so the tests mostly need to check that we're calling the right Spark functions and methods.

## Releasing

Set `version` in `build.clj` (a pre-release has a suffix, like `0.1.0-alpha.1`) and get it onto `develop`. Then tag that commit and push the tag:

```bash
git tag v0.1.0-alpha.1
git push origin v0.1.0-alpha.1
```

The Release workflow deploys `zero.one/geni` to Clojars, builds the CLI uberjar and attaches it to a GitHub release, marked as a pre-release when the version has a suffix. A failed run can be rerun: it skips the deploy when Clojars already has that version from the same commit. A published tag never moves, though, so that the tag, the Clojars jar and the uberjar always agree. If a release needs a fix, release a new version, such as `0.1.0-alpha.2`. The pom records the commit it was built from, and the workflow fails when Clojars has the version from another commit.

After a final release, and once its uberjar is on the release page, bump `resources/GENI_REPL_RELEASED_VERSION` on `develop`, so that installed `geni` scripts pick the new uberjar up. Scripts installed before 0.1.0 don't notice new releases: they need installing again, or `geni --force-download` once after each release.

The same build tasks run locally: `clojure -T:build deploy`, with `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` set, and `clojure -T:build cli-uber` for the uberjar.

## Brand assets

`assets/` holds the lockup, `geni-lockup.svg` (and `geni-lockup-dark.svg` for dark backgrounds), and the mark, `geni-mark.svg`. The lockup is the name in Outfit Medium, a hairline divider, and the name in Javanese script, ꦒꦼꦤꦶ, in Noto Sans Javanese Regular sized to Outfit's x-height. The mark is the first letter, ꦒ, in Noto Sans Javanese Bold. The colours are `#681A03` for the wordmark, `#8B432B` for the script and the divider (`#E3743A` on dark), and `#BE4F1C` at the base of the mark's gradient. Please use the SVGs as they are, rather than recolouring or restyling them.
