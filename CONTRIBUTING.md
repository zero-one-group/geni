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

On a pull request, the CI runs `:spark` and `:spark-4` on JDK 21, split into two shards with `:shard '[1 2]'` and `:shard '[2 2]'`, plus `:spark` on arm64. A push to `develop` also covers JDK 17 and `:spark-3.5-2.13`. Each run also installs the library jar and uses it from the README's `deps.edn`, on Clojure 1.11.4 and JDK 17, the oldest that Geni supports.

## Linting and formatting

```bash
clojure -T:build lint    # clj-kondo, then cljfmt's check
clojure -T:build fmt     # reformats the sources with cljfmt
clojure -T:build check   # lint, the tests on :spark, and the doc tests
```

`check` is what the CI runs on a pull request, so running it before you push saves a round trip. Both `lint` and `check` call `clojure -M:kondo` and `clojure -M:fmt` underneath, which take the same arguments as the tools themselves.

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

After a final release, bump `resources/GENI_REPL_RELEASED_VERSION` on `develop`, so that installed `geni` scripts pick the new uberjar up. Scripts installed before 0.1.0 keep their downloaded uberjar until they're run once with `geni --force-download`.

The same build tasks run locally: `clojure -T:build deploy`, with `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` set, and `clojure -T:build cli-uber` for the uberjar.

## Brand assets

`assets/` holds the lockup, `geni-lockup.svg` (and `geni-lockup-dark.svg` for dark backgrounds), and the mark, `geni-mark.svg`. The lockup is the name in Outfit Medium, a hairline divider, and the name in Javanese script, ꦒꦼꦤꦶ, in Noto Sans Javanese Regular sized to Outfit's x-height. The mark is the first letter, ꦒ, in Noto Sans Javanese Bold. The colours are `#681A03` for the wordmark, `#8B432B` for the script and the divider (`#E3743A` on dark), and `#BE4F1C` at the base of the mark's gradient. Please use the SVGs as they are, rather than recolouring or restyling them.
