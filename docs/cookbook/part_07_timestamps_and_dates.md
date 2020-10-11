# CB-07: Timestamps and Dates 

Spark (and thus Geni) has many timestamp and datetime functions - for more detail, check out [Spark's SQL functions docs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html). In this part, we look into one particular case of handling Unix timestamps. As usual, we get the data from the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%201%20-%20Reading%20from%20a%20CSV.ipynb) on the author's popularity-contest file. The explanation of the data can be found [here](http://popcon.ubuntu.com/README).

We download the data as in [part 2 of the cookbook](part_2_selecting_rows_and_columns.md):

```clojure
(def popularity-contest-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/popularity-contest")

(def popularity-contest-data-path
  "data/cookbook/popularity-contest.csv")

(download-data! popularity-contest-data-url popularity-contest-data-path)
=> :downloaded
```

We load, rename and remove the final row, which should not be part of the dataset:

```clojure
(def popularity-contest
  (-> (g/read-csv! popularity-contest-data-path {:delimiter " "})
      (g/to-df :access-time :creation-time :package-name :mru-program :tag)
      (g/remove (g/= :access-time (g/lit "END-POPULARITY-CONTEST-0")))))

(g/print-schema popularity-contest)
; root
;  |-- access-time: string (nullable = true)
;  |-- creation-time: string (nullable = true)
;  |-- package-name: string (nullable = true)
;  |-- mru-program: string (nullable = true)
;  |-- tag: string (nullable = true)

(-> popularity-contest (g/limit 5) g/show)
; +-----------+-------------+------------+--------------------------------------------+--------------+
; |access-time|creation-time|package-name|mru-program                                 |tag           |
; +-----------+-------------+------------+--------------------------------------------+--------------+
; |1387295797 |1367633260   |perl-base   |/usr/bin/perl                               |null          |
; |1387295796 |1354370480   |login       |/bin/su                                     |null          |
; |1387295743 |1354341275   |libtalloc2  |/usr/lib/x86_64-linux-gnu/libtalloc.so.2.0.7|null          |
; |1387295743 |1387224204   |libwbclient0|/usr/lib/x86_64-linux-gnu/libwbclient.so.0  |<RECENT-CTIME>|
; |1387295742 |1354341253   |libselinux1 |/lib/x86_64-linux-gnu/libselinux.so.1       |null          |
; +-----------+-------------+------------+--------------------------------------------+--------------+
```

## 7.1 Parsing Timestamps

The function `g/to-timestamp` expects an integer of the Unix timestamp. Since the times are parsed as strings, we must first cast the column to integer before invoking the function:

```clojure
(def formatted-popularity-contest
  (-> popularity-contest
      (g/with-column :access-time (g/to-timestamp (g/int :access-time)))
      (g/with-column :creation-time (g/to-timestamp (g/int :creation-time)))))

(g/print-schema formatted-popularity-contest)
; root
;  |-- access-time: timestamp (nullable = true)
;  |-- creation-time: timestamp (nullable = true)
;  |-- package-name: string (nullable = true)
;  |-- mru-program: string (nullable = true)
;  |-- tag: string (nullable = true)

(-> formatted-popularity-contest (g/limit 5) g/show)
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
; |access-time        |creation-time      |package-name|mru-program                                 |tag           |
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
; |2013-12-17 22:56:37|2013-05-04 09:07:40|perl-base   |/usr/bin/perl                               |null          |
; |2013-12-17 22:56:36|2012-12-01 21:01:20|login       |/bin/su                                     |null          |
; |2013-12-17 22:55:43|2012-12-01 12:54:35|libtalloc2  |/usr/lib/x86_64-linux-gnu/libtalloc.so.2.0.7|null          |
; |2013-12-17 22:55:43|2013-12-17 03:03:24|libwbclient0|/usr/lib/x86_64-linux-gnu/libwbclient.so.0  |<RECENT-CTIME>|
; |2013-12-17 22:55:42|2012-12-01 12:54:13|libselinux1 |/lib/x86_64-linux-gnu/libselinux.so.1       |null          |
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
```

## 7.2 Flagging Timestamp Zero

When we look into the distribution of the timestamps, a significant proportion of the time is in year 1970, which corresponds to [the epoch](https://en.wikipedia.org/wiki/Unix_time) or timestamp zero:

```clojure
(-> formatted-popularity-contest
    (g/select (g/year :access-time))
    g/value-counts
    g/show)
; +-----------------+-----+
; |year(access-time)|count|
; +-----------------+-----+
; |2013             |1861 |
; |1970             |799  |
; |2012             |203  |
; |2011             |28   |
; |2010             |5    |
; |2008             |1    |
; +-----------------+-----+
```

Luckily, timestamps support comparisons such as `g/<`:

```clojure
(def cleaned-popularity-contest
  (g/remove formatted-popularity-contest (g/< :access-time (g/to-timestamp 1))))

(-> cleaned-popularity-contest
    (g/select (g/year :access-time))
    g/value-counts
    g/show)
; +-----------------+-----+
; |year(access-time)|count|
; +-----------------+-----+
; |2013             |1861 |
; |2012             |203  |
; |2011             |28   |
; |2010             |5    |
; |2008             |1    |
; +-----------------+-----+
```
