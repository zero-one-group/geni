# CB-00: Getting Started with Clojure, Geni and Spark

## Clojure

This cookbook's syllabus is based on the popular [Pandas Cookbook](https://github.com/jvns/pandas-cookbook).

In the following sections, we shall assume a starting point of a clean install of a recent version of Ubuntu. It should be straightforward to find analogous commands for other Unix-based systems such as MacOS.

### Installation

Use JDK 21 and the [Clojure CLI](https://clojure.org/guides/install_clojure), as described in [CONTRIBUTING.md](../../CONTRIBUTING.md). The cookbook runs against the Spark 3.5 setup in the repository's `:spark` alias.

### Learning Resources

The [Brave Clojure](https://www.braveclojure.com/) book is available for free and provides a gentle introduction to Clojure. [The Joy of Clojure](https://www.manning.com/books/the-joy-of-clojure-second-edition) provides a more substantial treatment of the language.

Rich Hickey's paper [A History of Clojure](https://download.clojure.org/papers/clojure-hopl-iv-final.pdf) is particularly useful to understand the founding principles of the language and the problem it tries to solve. He has given helpful talks including [Clojure for Java Programmers](https://www.youtube.com/watch?v=P76Vbsk_3J0), [Clojure Made Simple](https://www.youtube.com/watch?v=VSdnJDO-xdg) and [Simple Made Easy](https://www.youtube.com/watch?v=oytL881p-nQ).

For paid resources, [Purely Functional TV](https://purelyfunctional.tv/) and [Lambda Island](https://lambdaisland.com/) are by far the most popular sources. John Stevenson's [Practicalli](http://practicalli.github.io/clojure/) has recently been picking up momentum as well.

As a matter of style, Geni heavily uses Clojure's threading macro `->`. A basic guide can be found [here](https://clojure.org/guides/threading_macros).

### Tooling

The Brave Clojure book has a good treatment of Emacs and Cider, which are the dominant IDE of choice for many Clojure developers. Many of the video demos on this guide uses [Neovim](https://neovim.io/) and [Conjure](https://oli.me.uk/getting-started-with-clojure-neovim-and-conjure-in-minutes/).

## Geni

From a checkout of Geni, prepare the Java classes and start a REPL:

```bash
clojure -T:build prep
clj -M:spark:test
```

Run examples from the repository root so that their relative paths resolve. Each runnable chapter requires its own namespaces. Downloads and generated datasets live under `data/cookbook/`; part 5 reads the weather dataset written by part 4. Parts 10 and 12 need manual dataset preparation and are excluded from the automated tests.

To run the automated cookbook examples:

```bash
clojure -T:build cookbook
```

The first run downloads the public datasets. Later runs reuse them. See [CONTRIBUTING.md](../../CONTRIBUTING.md#executable-documentation) for how to check and refresh the documented outputs.

## Spark

[Apache Spark](https://spark.apache.org/) is a popular distributed data processing library written natively in Scala. Geni supports Spark 3.5 and Spark 4 and provides interfaces for [Spark SQL](https://spark.apache.org/sql/) and [Spark ML](https://spark.apache.org/mllib/). Many functionalities of Spark SQL and ML are supported, and it can be helpful to refer to the [original Spark docs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html) as reference. The translation from original Spark functions or methods to Geni functions should, in most cases, be as simple as translating camel case to kebab case.
