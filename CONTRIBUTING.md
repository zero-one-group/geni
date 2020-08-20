# Contributing to Geni

First and foremost, thank you for taking an interest to contribute to Geni. This project can only improve with your help!

> First-time contributor? You are most welcomed! Check out the [Open Source Guide](https://opensource.guide/)!

## Contributions

All kinds of contributions are welcomed! 

If you would like to **report an issue or request a new feature**, please [create a new issue](https://github.com/zero-one-group/geni/issues). If you have a question, please reach out to the Clojurians Slack channel #geni. Finally, to avoid duplication of work, please let us know if you would like to have a crack on the issue by leaving a comment on the issue!

Geni aims to cover as many of **Spark's features** as possible. If you find a missing feature, please report it as an issue. Ideally it would come with a use case and a link to the [original Spark documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html) as references.

Geni also aims to be an idiomatic Clojure library. If you have any suggestions on **Clojure idioms** or metaphors that would apply to Spark, please let us know!

Any help on **documentation and tutorials** would also be great! Linking the Clojure function with the [original Spark documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html) can be a great help. Adding any [examples](examples/) or [discussion points](docs/) will improve the getting-started experience of Geni.

## Contributing Walkthrough

To contribute to Geni, please fork the [main repository](https://github.com/zero-one-group/geni) and submit a pull request. A maintainer should pick up the pull request and review the changes, which hopefully gets approved and merged to the develop branch!

To make sure that the project builds correctly, clone your forked repository and run `make ci` (or `make ci --jobs 3` on a bigger machine). This should build a docker container, report the test coverage using [cloverage](https://github.com/cloverage/cloverage), check for linting errors using [clj-kondo](https://github.com/borkdude/clj-kondo), check that no artifacts are out of date using [lein-ancient](https://github.com/xsc/lein-ancient), run the unit tests on the lein template, test the installation of the Geni CLI and test the Geni REPL uberjar.

As much as possible, we would like to keep the test coverage close to (and ideally at) 100%. Much of the actual data work is done by Spark, so it should be sufficient to check that we are calling the right Spark functions and methods. There should also be zero lint errors and warnings.

Once you are happy with the changes, run `make ci` once again to check that all the CI steps pass. Every pull request will additionally trigger [actions](https://github.com/zero-one-group/geni/blob/develop/.github/workflows/continuous-integration.yml) to check these conditions.

## Development Tools

Geni uses [Midje](https://github.com/marick/Midje) as its test runner. It allows us to do watch and run our unit tests should there be any changes to the source code. Midje is smart enough to load namespaces that are required by the changed file.

Additionally, there are unit tests that are marked as "slow", which may not be relevant if you are working on a new, orthogonal feature or bug fix. To watch and run non-slow tests, we could either run:

```bash
lein midje :autotest :filter -slow
```

or spin up a REPL by running `lein repl` and run:

```clojure
(require '[midje.repl :refer [autotest]])
(autotest :filter (complement :slow))
```

## Release Checklist

- Bump the versions in:
    - `project.clj`.
    - `lein-template`'s `project.clj`;
    - `lein-template`'s `resources/.../project.clj`; and
    - `resources/GENI_REPL_RELEASED_VERSION`.
- Ensure that the CI steps pass with `make ci`.
- Deploy the main library with `lein deploy clojars`.
- Push the newly built container to DockerHub with `make docker-push`.
- Merge the library version-bump branch.
- Create a new release on GitHub with a summary of all the changes and the new uberjar uploaded.
