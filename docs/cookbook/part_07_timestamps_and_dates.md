# CB-07: Timestamps and Dates 

Spark (and thus Geni) has many timestamp and datetime functions - for more detail, check out [Spark's SQL functions docs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html). In this part, we look into one particular case of handling Unix timestamps. As usual, we get the data from the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%201%20-%20Reading%20from%20a%20CSV.ipynb) on the author's popularity-contest file. The explanation of the data can be found [here](http://popcon.ubuntu.com/README).

As in every part, we start with Geni and the `download-data!` function from [part 1](part_01_reading_and_writing_datasets.md):

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])

(defn download-data! [source-url target-path]
  (if (.exists (io/file target-path))
    :already-exists
    (do
      (io/make-parents target-path)
      (with-open [in (io/input-stream source-url)]
        (io/copy in (io/file target-path)))
      :downloaded)))
```

Then we download the data:

```clojure
(def popularity-contest-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/80817f5a46ef6d2f8444eeb7181e00c0aabb5a57/cookbook/data/popularity-contest")

(def popularity-contest-data-path
  "data/cookbook/popularity-contest.csv")

(download-data! popularity-contest-data-url popularity-contest-data-path)
; :downloaded
```

We load, rename and remove the final row, which should not be part of the dataset:

```clojure
(def popularity-contest
  (-> (g/read-csv! popularity-contest-data-path {:delimiter " "})
      (g/to-df :access-time :creation-time :package-name :mru-program :tag)
      (g/remove (g/= :access-time (g/lit "END-POPULARITY-CONTEST-0")))))

(g/print-schema popularity-contest)
;; =stdout=>
; root
;  |-- access-time: string (nullable = true)
;  |-- creation-time: string (nullable = true)
;  |-- package-name: string (nullable = true)
;  |-- mru-program: string (nullable = true)
;  |-- tag: string (nullable = true)

(-> popularity-contest (g/limit 5) g/show)
;; =stdout=>
; +-----------+-------------+------------+--------------------------------------------+--------------+
; |access-time|creation-time|package-name|mru-program                                 |tag           |
; +-----------+-------------+------------+--------------------------------------------+--------------+
; |1387295797 |1367633260   |perl-base   |/usr/bin/perl                               |NULL          |
; |1387295796 |1354370480   |login       |/bin/su                                     |NULL          |
; |1387295743 |1354341275   |libtalloc2  |/usr/lib/x86_64-linux-gnu/libtalloc.so.2.0.7|NULL          |
; |1387295743 |1387224204   |libwbclient0|/usr/lib/x86_64-linux-gnu/libwbclient.so.0  |<RECENT-CTIME>|
; |1387295742 |1354341253   |libselinux1 |/lib/x86_64-linux-gnu/libselinux.so.1       |NULL          |
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
;; =stdout=>
; root
;  |-- access-time: timestamp (nullable = true)
;  |-- creation-time: timestamp (nullable = true)
;  |-- package-name: string (nullable = true)
;  |-- mru-program: string (nullable = true)
;  |-- tag: string (nullable = true)

(-> formatted-popularity-contest (g/limit 5) g/show)
;; =stdout=>
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
; |access-time        |creation-time      |package-name|mru-program                                 |tag           |
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
; |2013-12-17 15:56:37|2013-05-04 02:07:40|perl-base   |/usr/bin/perl                               |NULL          |
; |2013-12-17 15:56:36|2012-12-01 14:01:20|login       |/bin/su                                     |NULL          |
; |2013-12-17 15:55:43|2012-12-01 05:54:35|libtalloc2  |/usr/lib/x86_64-linux-gnu/libtalloc.so.2.0.7|NULL          |
; |2013-12-17 15:55:43|2013-12-16 20:03:24|libwbclient0|/usr/lib/x86_64-linux-gnu/libwbclient.so.0  |<RECENT-CTIME>|
; |2013-12-17 15:55:42|2012-12-01 05:54:13|libselinux1 |/lib/x86_64-linux-gnu/libselinux.so.1       |NULL          |
; +-------------------+-------------------+------------+--------------------------------------------+--------------+
```

## 7.2 Flagging Timestamp Zero

When we look into the distribution of the timestamps, a significant proportion of the time is in year 1970, which corresponds to [the epoch](https://en.wikipedia.org/wiki/Unix_time) or timestamp zero:

```clojure
(-> formatted-popularity-contest
    (g/select (g/year :access-time))
    g/value-counts
    g/show)
;; =stdout=>
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
;; =stdout=>
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
