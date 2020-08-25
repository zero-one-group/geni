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
                    ['brand-id']
                    .value_counts())
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

First Class Clojure, First Class Spark

## Easy Getting-Started Story
