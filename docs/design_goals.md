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

In [3]: df.groupby('Regionname').mean()['Price'].sort_values()
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
λ (-> df (g/group-by :Regionname) (g/agg {:price (g/mean :Price)}) (g/sort :price) g/show)
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

After timing a personal run, the Python-Pandas version took 24 seconds, whereas the Clojure-Geni version took 34 seconds. Although, the Python-Pandas combination has a small edge for sub-one-minute tasks, the Clojure-Geni combination has all the Clojure REPL facilities including tight text-editor integrations, which makes for a better REPL for bigger tasks.

## Data Wrangling Performance

## First Class Clojure, First Class Spark

## Easy Getting-Started Story
