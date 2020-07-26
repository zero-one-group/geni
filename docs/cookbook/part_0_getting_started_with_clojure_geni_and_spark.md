# CB0: Getting Started with Clojure, Geni and Spark

## Clojure

This cookbook's syllabus is based on the popular [Pandas Cookbook](https://github.com/jvns/pandas-cookbook).

In the following sections, we shall assume a starting point of a clean install of a recent version of Ubuntu. It should be straightforward to find analogous commands for other Unix-based systems such as MacOS.

### Installation

Clojure requires Java. The latest LTS JDK version that Spark 3.0.0 is JDK 11. For that reason, we recommend using JDK 11. The following command should do the trick:

```bash
wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
sudo add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
sudo apt update
sudo apt install -y adoptopenjdk-11-openj9
```

This guide uses [Leiningen](https://leiningen.org/) as the main Clojure build tool. To install Leiningen, follow the install instruction on the [Leininingen website](https://leiningen.org/). Run the following command to install `lein` at `/opt/lein`:

```bash
sudo mkdir -p /opt
curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
sudo mv lein /opt/
sudo chmod a+x /opt/lein
sudo mkdir -p $HOME/.lein
```

Once installed, we have the `lein` command available on the terminal. To launch a regular Clojure REPL, simply run `lein repl`.

We will be using `lein` throughout this cookbook, but it is worth mentioning the other main Clojure build tool, namely [Clojure command line tools](https://clojure.org/guides/deps_and_cli). Installation instructions can be found [here](https://clojure.org/guides/getting_started).

### Learning Resources

The [Brave Clojure](https://www.braveclojure.com/) book is available for free and provides a gentle introduction to Clojure. [The Joy of Clojure](https://www.manning.com/books/the-joy-of-clojure-second-edition) provides a more substantial treatment of the language.

Rich Hickey's paper [A History of Clojure](https://download.clojure.org/papers/clojure-hopl-iv-final.pdf) is particularly useful to understand the founding principles of the language and the problem it tries to solve. He has given helpful talks including [Clojure for Java Programmers](https://www.youtube.com/watch?v=P76Vbsk_3J0), [Clojure Made Simple](https://www.youtube.com/watch?v=VSdnJDO-xdg) and [Simple Made Easy](https://www.youtube.com/watch?v=oytL881p-nQ).

For paid resources, [Purely Functional TV](https://purelyfunctional.tv/) and [Lambda Island](https://lambdaisland.com/) are by far the most popular sources. John Stevenson's [Practicalli](http://practicalli.github.io/clojure/) has recently been picking up momentum as well.

As a matter of style, Geni heavily uses Clojure's threading macro `->`. A basic guide can be found [here](https://clojure.org/guides/threading_macros).

### Tooling

The Brave Clojure book has a good treatment of Emacs and Cider, which are the dominant IDE of choice for many Clojure developers. Many of the video demos on this guide uses [Neovim](https://neovim.io/) and [Conjure](https://oli.me.uk/getting-started-with-clojure-neovim-and-conjure-in-minutes/).

## Geni

The easiest way to get started with Geni is to start from the [lein template](https://github.com/zero-one-group/geni-template):

```bash
lein new geni geni-cookbook
```

Leiningen will handle all the JVM dependencies including Spark and Geni itself through the settings file project.clj. To check whether the templated application behaves correctly, run:

```bash
cd geni-cookbook && lein run
```

The first time running the command may take a while as Leiningen resolves and fetches the required dependencies. Once it is done, it executes the `src/geni-cookbook/core.clj` source file, where it runs a dummy [Spark ML](http://spark.apache.org/docs/latest/ml-guide.html) example and steps into a REPL. It should also print the Spark-session creation log and something along the lines of:

```bash
{:spark.app.name "Geni App",
 :spark.driver.port "59073",
 :spark.master "local[*]",
 :spark.executor.id "driver",
 :spark.driver.host "192.168.101.123",
 :spark.app.id "local-1594972734929"}
+---+------------------+----------------------------------------+----------+
|id |text              |probability                             |prediction|
+---+------------------+----------------------------------------+----------+
|4  |spark i j k       |[0.15964077387874104,0.8403592261212589]|1.0       |
|5  |l m n             |[0.8378325685476612,0.16216743145233875]|0.0       |
|6  |spark hadoop spark|[0.06926633132976262,0.9307336686702374]|1.0       |
|7  |apache hadoop     |[0.9821575333444208,0.01784246665557917]|0.0       |
+---+------------------+----------------------------------------+----------+
```

From here on, we can start editing the `src/geni-cookbook/core.clj` file and execute the code on the REPL!

[![asciicast](https://asciinema.org/a/346987.svg)](https://asciinema.org/a/346987?speed=1.75)

## Spark

[Apache Spark](https://spark.apache.org/) is a popular distributed data processing library written natively in Scala. Geni supports [Spark 3](https://spark.apache.org/releases/spark-release-3-0-0.html) and provides interfaces for [Spark SQL](https://spark.apache.org/sql/) and [Spark ML](https://spark.apache.org/mllib/). Many functionalities of Spark SQL and ML are supported, and it can be helpful to refer to the [original Spark docs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html) as reference. The translation from original Spark functions or methods to Geni functions should, in most cases, be as simple as translating camel case to kebab case.
