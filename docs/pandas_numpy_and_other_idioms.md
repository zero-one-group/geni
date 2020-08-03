# Pandas, NumPy and Other Idioms

Occasionally, we find functions from other languages and libraries that solve certain problems succinctly and elegantly, but are unfortunately absent in Spark. Geni aims to incorporate many such functions in order to speed up the feedback loop and improve the REPL experience.

This doc discusses a few interesting examples of such imported idioms. It is by no means exhaustive. See the [cljdoc](https://cljdoc.org/d/zero.one/geni/CURRENT) entry for the `zero-one.geni.foreign-idioms` namespace for the complete set.

## Pandas

Sometimes it's convenient to know the shape of a dataframe:

```clojure
(g/shape dataframe)
=> [13580 21]
```

The function [`value-counts`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.value_counts.html) returns the counts of unique values. Unlike in Pandas, this operation does not require a single column:

```clojure
(-> dataframe
    (g/select :SellerG :Suburb)
    g/value-counts
    (g/limit 5)
    g/show)
; +-------+--------------+-----+
; |SellerG|Suburb        |count|
; +-------+--------------+-----+
; |Biggin |Richmond      |101  |
; |Nelson |Essendon      |97   |
; |Nelson |Brunswick     |96   |
; |Barry  |Reservoir     |90   |
; |Buxton |Bentleigh East|88   |
; +-------+--------------+-----+
```

The function [`cut`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.cut.html) bins values into discrete intervals:

```clojure
(-> dataframe
    (g/select {:price-bins (g/cut :Price [8e5 1e6 1.2e6])})
    g/value-counts
    g/show)
; +---------------------------+-----+
; |price-bins                 |count|
; +---------------------------+-----+
; |Price[-Infinity, 800000.0] |5479 |
; |Price[1200000.0, Infinity] |4328 |
; |Price[800000.0, 1000000.0] |2358 |
; |Price[1000000.0, 1200000.0]|1415 |
; +---------------------------+-----+
```

At times, it may be convenient to do the binning based on the quantiles. Enter [`qcut`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.qcut.html):

```clojure
(-> dataframe
    (g/select {:price :Price
               :bins  (g/qcut :Price [0.1 0.5 0.9])})
    (g/group-by :bins)
    (g/agg {:min   (g/min :price)
            :mean  (g/mean :price)
            :max   (g/max :price)
            :count (g/count "*")})
    g/show)
; +---------------+---------+------------------+---------+-----+
; |bins           |min      |mean              |max      |count|
; +---------------+---------+------------------+---------+-----+
; |Price[0.0, 0.1]|85000.0  |392448.1684981685 |480000.0 |1365 |
; |Price[0.1, 0.5]|480500.0 |697254.5108695652 |903000.0 |5428 |
; |Price[0.5, 0.9]|904000.0 |1270420.509825528 |1850000.0|5445 |
; |Price[0.9, 1.0]|1851000.0|2511148.2026825636|9000000.0|1342 |
; +---------------+---------+------------------+---------+-----+
```

Or simply just, say, five equal bins:

```clojure
(-> dataframe
    (g/select {:price :Price
               :bins  (g/qcut :Price 5)})
    (g/group-by :bins)
    (g/agg {:min   (g/min :price)
            :mean  (g/mean :price)
            :max   (g/max :price)
            :count (g/count "*")})
    g/show)
; +---------------+---------+------------------+---------+-----+
; |bins           |min      |mean              |max      |count|
; +---------------+---------+------------------+---------+-----+
; |Price[0.0, 0.2]|85000.0  |469287.6683562636 |600000.0 |2762 |
; |Price[0.2, 0.4]|600500.0 |700734.7062937063 |800000.0 |2717 |
; |Price[0.4, 0.6]|800001.0 |911216.2482269504 |1040000.0|2679 |
; |Price[0.6, 0.8]|1041000.0|1240686.2796330275|1450000.0|2725 |
; |Price[0.8, 1.0]|1451000.0|2071079.9484612532|9000000.0|2697 |
; +---------------+---------+------------------+---------+-----+
```

## NumPy

NumPy's [`clip`](https://numpy.org/doc/stable/reference/generated/numpy.clip.html) can sometimes be pretty handy:

```clojure
(-> dataframe
    (g/select {:land-size :LandSize
               :clipped   (g/clip :LandSize 100 200)})
    (g/limit 10)
    g/show)
; +---------+-------+
; |land-size|clipped|
; +---------+-------+
; |202.0    |200.0  |
; |156.0    |156.0  |
; |134.0    |134.0  |
; |94.0     |100.0  |
; |120.0    |120.0  |
; |181.0    |181.0  |
; |245.0    |200.0  |
; |256.0    |200.0  |
; |0.0      |100.0  |
; |220.0    |200.0  |
;+---------+-------+
```

## tech.ml.dataset

Users of TechAscent's [dataset](https://github.com/techascent/tech.ml.dataset) may be more familiar with the `->dataset` function for dataframe creation:

```clojure
(-> [{:a 1 :b 2} {:a 2 :c 3}]
    g/->dataset
    g/show)
; +---+----+----+
; |a  |b   |c   |
; +---+----+----+
; |1  |2   |null|
; |2  |null|3   |
; +---+----+----+
```

And it works with file paths and options:

```clojure
(-> "test/resources/melbourne_housing_snapshot.parquet"
    (g/->dataset {:n-records 5 :column-whitelist [:Price 
                                                  :Suburb 
                                                  :LandSize]})
    g/show)
; +---------+----------+--------+
; |Price    |Suburb    |LandSize|
; +---------+----------+--------+
; |1480000.0|Abbotsford|202.0   |
; |1035000.0|Abbotsford|156.0   |
; |1465000.0|Abbotsford|134.0   |
; |850000.0 |Abbotsford|94.0    |
; |1600000.0|Abbotsford|120.0   |
; +---------+----------+--------+
```
