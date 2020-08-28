# Design Goals

Geni is designed primarily to be a good data-analysis tool that is optimised for frequent and rapid feedback from the data. The core design of Geni is informed by our personal experience working as a data scientist that requires asking many questions about the data and writing countless queries for it.

## Fast, Accessible REPL

This is important when an idea randomly pops up, and we would like to know the answer **here and now**. The key here is to have a dataframe library that is accessible through a fast-starting REPL from any directory on the terminal. Geni's answer to this is the Geni CLI, which is essentially an executable script that starts a Clojure REPL (as well as an nREPL server), requires Geni namespaces and instantiates a `SparkSession` in parallel.

With Clojure and Spark sub-optimal startup times, Geni is clearly handicapped compared to R and Python. On our machine, the startup times are as follows:

| Library/Language | Startup Time (s) | Command                                       |
| :---:            | :---:            | :---                                          |
| R                | 0.2              | `time bash -c "exit \| R --no-save"`          |
| Python           | 0.3              | `time bash -c "exit \| ipython"`              |
| Geni             | 7.3              | `time bash -c "exit \| geni"`                 |
| Spark Shell      | 8.4              | `time bash -c "echo sys.exit \| spark-shell"` |

It is clearly not as fast-starting as R and Python, but it is still good to use for sub-one-minute tasks. To illustrate this, suppose that we are working with the Melbourne housing dataset stored in `data/melbourne.parquet`, and we would like to know which region has the highest mean house price. Consider the following Python and Clojure snippets:

<table>
    <tr>
        <th align="center" width="441">
            Python-Pandas
        </th>
        <th align="center" width="441">
            Clojure-Geni
        </th>
    </tr>
    <tr>
<td>
<pre>
$ ipython
...
In [1]: import pandas as pd

In [2]: df = pd.read_parquet('data/melbourne.parquet')

In [3]: (df.groupby('Regionname')
   ...:    .mean()['Price']
   ...:    .sort_values())
Out[3]:
Regionname
Western Victoria              3.975234e+05
Northern Victoria             5.948293e+05
Eastern Victoria              6.999808e+05
Western Metropolitan          8.664205e+05
Northern Metropolitan         8.981711e+05
South-Eastern Metropolitan    9.229438e+05
Eastern Metropolitan          1.104080e+06
Southern Metropolitan         1.372963e+06
...
</pre>
</td>
<td>
<pre>
$ geni
...
geni-repl (user)
λ (def df (g/read-parquet! "data/melbourne.parquet"))
#'user/df
geni-repl (user)
λ (-> df
      (g/group-by :Regionname)
      (g/agg {:price (g/mean :Price)})
      (g/sort :price)
      g/show)
+--------------------------+------------------+
|Regionname                |price             |
+--------------------------+------------------+
|Western Victoria          |397523.4375       |
|Northern Victoria         |594829.268292683  |
|Eastern Victoria          |699980.7924528302 |
|Western Metropolitan      |866420.5200135686 |
|Northern Metropolitan     |898171.0822622108 |
|South-Eastern Metropolitan|922943.7844444445 |
|Eastern Metropolitan      |1104079.6342624065|
|Southern Metropolitan     |1372963.3693290735|
+--------------------------+------------------+
...
</pre>
</td>
    </tr>
</table>

After timing a personal run, the Python-Pandas version took 24 seconds, whereas the Clojure-Geni version took 34 seconds. The Python-Pandas combination has a small edge for sub-one-minute tasks, but the Clojure-Geni combination has all the Clojure REPL facilities including tight text-editor integrations. These make for a better REPL experience for bigger tasks.

## Data Wrangling Performance

One downside to the Python-Pandas combination is that the latter is single-threaded. This means that Pandas performance is very slow compared to other libraries for easily parallelisable tasks. To illustrate this point, consider [the dummy retail data](../examples/performance_benchmark_data.clj) with 24 million transactions and over one million customers. Suppose that we would like to know how many transactions do the top brands have:

<table>
    <tr>
        <th align="center" width="441">
            Python-Pandas
        </th>
        <th align="center" width="441">
            Clojure-Geni
        </th>
    </tr>
    <tr>
<td>
<pre>
$ ipython
...
In [1]: import pandas as pd

In [2]: %time (pd.read_parquet('data/dummy_retail')
                    ['brand-id'].value_counts())
CPU times: user 11 s, sys: 5.11 s, total: 16.1 s
Wall time: 12.9 s
Out[2]:
0       238757
1       236314
2       233277
3       231845
4       229180
         ...
1224         1
1222         1
1218         1
1213         1
1826         1
Name: brand-id, Length: 1310, dtype: int64
...
</pre>
</td>
<td>
<pre>
$ geni
...
λ (time (-> (g/read-parquet! "data/dummy_retail")
            (g/select "brand-id")
            g/value-counts
            g/show))
+--------+------+
|brand-id|count |
+--------+------+
|0       |238757|
|1       |236314|
|2       |233277|
|3       |231845|
|4       |229180|
|5       |226255|
|6       |225069|
|7       |222698|
|8       |220850|
|9       |217840|
+--------+------+
"Elapsed time: 3447.6941 msecs"
...
</pre>
</td>
    </tr>
</table>

In this case, we see around 3.7x performance for a very simple query. However, for more substantial queries, the speedups are typical greater - even up to 73x. See [the simple performance benchmark post](simple_performance_benchmark.md) for a more detailed treatment.

## Seamless Parasitism

<blockquote>
    <p> 
        Clojure is intentionally hosted, in that it compiles to and runs on the runtime of another language, such as the JVM. This is more than an implementation strategy; numerous features ensure that programs written in Clojure can leverage and interoperate with the libraries of the host language directly and efficiently.
    </p>
    &mdash;
    <a href="https://download.clojure.org/papers/clojure-hopl-iv-final.pdf">
        Rich Hickey, A History of Clojure.
    </a>
</blockquote>

Similar to Clojure's JVM parasitism, Geni aims to leverage the mature runtime and rich features of [Apache Spark](https://spark.apache.org/). This means that all of the hardwork in perfomance optimisation and computational heavy lifting are being taken care of by the Spark team, and Geni's job is to simply provide an ergonomic Clojure interface for the underlying Spark machinery. Just like how Clojure goes the extra mile with special host interop syntax, Geni tries to provide both a first-class Clojure experience **and** a first-class Spark experience.

<blockquote>
    <p> 
        React Native isn't our problem. That's somebody else's problem. We're just the little spider in the colony taking advantage of everything else that everybody else is doing.
    </p>
    &mdash;
    <a href="https://youtu.be/tX4wg4wOFuU?t=1661">
        David Nolen, Parasitic Programming Languages
    </a>
</blockquote>

Typically, this means translating Scala and Spark concepts into idiomatic Clojure whilst still leaving the door open for direct Scala interop. For instance, the `.groupBy` method on a Spark `Dataset` takes in an array of `Column`s to type check. The following snippet is valid Spark in Clojure:

```clojure
(import '(org.apache.spark.sql functions Column))

(-> dataframe
    (.groupBy (into-array Column [(functions/lower (functions/col "SellerG"))
                                  (functions/col "Suburb")
                                  (functions/col "Regionname")]))
    (.agg
      (.as (functions/mean "Price") "mean")
      (into-array Column [(.as (functions/stddev "Price") "std")
                          (.as (functions/min "Price") "min")
                          (.as (functions/max "Price") "max")]))
    .show)
```

However, it is problematic in (at least) two ways. Firstly, it is quite awkward to write a lot of Clojure code that looks like the above. It is partly due to the fact that Clojure is a dynamic language, and having to write boilerplate code in order to make the types line up feels unidiomatic. Secondly, it is quite verbose and thus makes for a poor REPL experience for rapid, throwaway queries. In contrast, we would write the following instead in Geni:

```clojure
(-> dataframe
    (g/group-by (g/lower :SellerG) :Suburb :Regionname)
    (g/agg {:mean (g/mean :Price)   
            :std  (g/stddev :Price)
            :min  (g/min :Price)
            :max  (g/max :Price)})
    g/show)
```

which is brief and clean enough to write spontaneously on the REPL. Notice that we can mix `Column`s and keywords on a single `g/group-by` call, as Geni converts everything to an array of Columns in the background. Since Geni does not wrap Spark data structures in other data structures, the direct-interop forms can be arbitrarily combined with the Geni forms within the same threading macro invocation.

Furthermore, to make it easier to compose with other Clojure libraries, functions such as `g/collect` returns Clojure-friendly data structures. What this means is that Scala maps are converted to Clojure maps, Scala tuples into Clojure vectors, Spark Rows to Clojure maps, Spark vectors to Clojure vectors, etc. Nested data structures are converted in the same way:

```clojure
(-> dataframe
    (g/select
      {:property
       (g/struct
         {:market   (g/struct :SellerG :Price :Date)
          :house    (g/struct :Landsize :Rooms)
          :location (g/struct :Address {:coord (g/struct :Lattitude :Longtitude)})})})
    (g/limit 1)
    g/collect)
=> ({:property
     {:market {:SellerG "Biggin", :Price 1480000.0, :Date "3/12/2016"},
      :house {:Landsize 202.0, :Rooms 2},
      :location
      {:Suburb "Abbotsford",
       :Address "85 Turner St",
       :coord {:Lattitude -37.7996, :Longtitude 144.9984}}}})
```

This also works the other way. For example, the RDD `.mapToPair` method requires a function that spits out a `scala.Tuple2` to type check. Geni's `rdd/map-to-pair` expects a function that returns a vector of length two instead, does the coercion to `scala.Tuple2` in the background, and throws an error should the coercion fail.

```clojure
;; We can write the following:
(rdd/map-to-pair rdd (fn [x] [x 1]))

;; instead of the direct interop as follows:
(import '(scala Tuple2))
(.mapToPair rdd (fn [x] (Tuple2. x 1))
```

## Easy Getting-Started Experience

Getting started with Geni should be easy not only for seasoned Clojure developers, but also for someone new to the language, who is perhaps just trying out the library. Speaking from personal experience, as a beginner to Clojure, having to install [Leiningen](https://leiningen.org/) or [Clojure CLI](https://clojure.org/guides/deps_and_cli) can be a turn off and an unnecessary barrier to entry - not to mention the [Emacs-Cider](https://www.braveclojure.com/basic-emacs/) combo that appears in many Clojure tutorials.

Geni's getting-started journey draws a lot from [borkdude](https://github.com/borkdude)'s work. In particular, [clj-kondo](https://github.com/borkdude/clj-kondo/blob/master/doc/install.md#installation-script-macos-and-linux) can be installed as a standalone application with three lines of Bash or a simple `brew install`. We can also install and run a [babashka](https://github.com/borkdude/babashka#quickstart) script in three lines of Bash. To achieve a similar effect, Geni is released not only as a Clojure library, but also as a command-line app that auto-downloads the latest Geni uberjar and defaults to a REPL (with an nREPL server) with all of the Geni namespaces required. However, instead of a standalone executable, it still requires `java`. Concretely, after running the [three-liner](https://github.com/zero-one-group/geni#install-geni) to install the Geni CLI, we can run a minimal application that prints the Spark session as follows:

```bash
$ echo "(clojure.pprint/pprint (g/spark-conf @spark))\n exit" | geni
```

<img src="https://media.giphy.com/media/lTAufFljfjXQgctqzI/giphy.gif" width="200">

For Leiningen users, there is also a lein template that creates a Geni application that runs a Spark ML example. For example:

```bash
$ lein new geni geni-app && cd geni-app && lein run
```

The template comes with `core.clj` that contains an example of a very simple application that uses the library and `core_test.clj` that unit-tests the Spark machinery running underneath.
