# Design Goals

Geni is designed primarily to be a good data-analysis tool that is optimised for frequent and rapid feedback from the data. The core design of Geni is informed by my personal experience working as a data scientist that requires asking many questions about the data and writing countless queries for it.

## Fast REPL

This is important when an idea randomly pops up, and we would like to know the answer here and now. The key is to have a dataframe library accessible through a fast-starting REPL from anywhere.

With Clojure and Spark sub-optimal startup times, Geni is clearly handicapped. On my machine, the startup times are as follows:

| Command | Runtime (s) |
| ---: | :---: |
| `time bash -c "exit \| R --no-save"` | 0.2 |
| `time bash -c "exit \| ipython"` | 0.3 |
| `time bash -c "exit \| geni"` | 7.3 |
| `time bash -c "echo sys.exit \| spark-shell"` | 8.4 |

It is clearly not as good as R and Python, but it is still bearable for sub-one-minute tasks. As an illustration, suppose that we are working with the Melbourne housing dataset stored in `data/melbourne.parquet`, and we would like to know which region has the highest mean house price. In Python we would do:

```
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
```

After timing myself, going in and out of the IPython REPL took 24 seconds. The following Geni version took 34 seconds, which is quite competitive to the Python version.

```
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
```

## Data Wrangling Performance

## First Class Clojure, First Class Spark

## Easy Getting-Started Story
