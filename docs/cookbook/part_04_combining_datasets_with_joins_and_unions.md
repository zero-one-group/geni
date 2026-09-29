# CB-04: Combining Datasets with Joins and Unions

We use the Canadian weather data archived in the [Pandas Cookbook](https://github.com/jvns/pandas-cookbook). A pinned copy keeps the examples reproducible when the live weather download service changes.

## 4.1 Loading One Month of Data

Start with Geni and the download helper from part 1:

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

(download-data!
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/80817f5a46ef6d2f8444eeb7181e00c0aabb5a57/cookbook/data/weather_2012.csv"
  "data/cookbook/weather-source-2012.csv")

(def weather-source
  (-> (g/read-csv! "data/cookbook/weather-source-2012.csv")
      (g/to-df :date-time :temp :dew-point-temp :rel-hum :wind-spd
               :visibility :stn-press :weather)
      (g/with-column :date-time (g/to-timestamp :date-time))
      (g/with-column :year (g/year :date-time))
      (g/with-column :month (g/month :date-time))
      (g/with-column :day (g/day-of-month :date-time))))

(defn weather-data [year month]
  (g/filter weather-source (g/&& (g/= :year year) (g/= :month month))))

(def raw-weather-mar-2012 (weather-data 2012 3))

(g/count raw-weather-mar-2012)
;; => 744

(g/print-schema raw-weather-mar-2012)
;; =stdout=>
; root
;  |-- date-time: timestamp (nullable = true)
;  |-- temp: double (nullable = true)
;  |-- dew-point-temp: double (nullable = true)
;  |-- rel-hum: integer (nullable = true)
;  |-- wind-spd: integer (nullable = true)
;  |-- visibility: double (nullable = true)
;  |-- stn-press: double (nullable = true)
;  |-- weather: string (nullable = true)
;  |-- year: integer (nullable = true)
;  |-- month: integer (nullable = true)
;  |-- day: integer (nullable = true)
```

## 4.2 Checking Columns for Nulls

Before combining datasets, check which columns contain nulls. Use `g/agg` and `g/null-count` to count missing values in each column:

```clojure
(def null-counts
  (-> raw-weather-mar-2012
      (g/agg (into {} (map #(vector % (g/null-count %))
                           (g/columns raw-weather-mar-2012))))
      g/first))

null-counts
;; => {:day 0,
;;     :temp 0,
;;     :wind-spd 0,
;;     :date-time 0,
;;     :stn-press 0,
;;     :month 0,
;;     :dew-point-temp 0,
;;     :year 0,
;;     :rel-hum 0,
;;     :weather 0,
;;     :visibility 0}
```

This archived dataset has already been cleaned. The same approach can select columns without nulls in a less complete dataset, while preserving their original order:

```clojure
(def columns-without-nulls
  (->> null-counts
       (filter #(zero? (second %)))
       (map first)
       set))

(def columns-to-select
  (filter columns-without-nulls (g/columns raw-weather-mar-2012)))

columns-to-select
;; => (:date-time
;;     :temp
;;     :dew-point-temp
;;     :rel-hum
;;     :wind-spd
;;     :visibility
;;     :stn-press
;;     :weather
;;     :year
;;     :month
;;     :day)

(def weather-mar-2012 (g/select raw-weather-mar-2012 columns-to-select))

(-> weather-mar-2012 (g/order-by :date-time) (g/limit 5) g/show)
;; =stdout=>
; +-------------------+----+--------------+-------+--------+----------+---------+-------+----+-----+---+
; |date-time          |temp|dew-point-temp|rel-hum|wind-spd|visibility|stn-press|weather|year|month|day|
; +-------------------+----+--------------+-------+--------+----------+---------+-------+----+-----+---+
; |2012-03-01 00:00:00|-5.5|-9.7          |72     |24      |4.0       |100.97   |Snow   |2012|3    |1  |
; |2012-03-01 01:00:00|-5.7|-8.7          |79     |26      |2.4       |100.87   |Snow   |2012|3    |1  |
; |2012-03-01 02:00:00|-5.4|-8.3          |80     |28      |4.8       |100.8    |Snow   |2012|3    |1  |
; |2012-03-01 03:00:00|-4.7|-7.7          |79     |28      |4.0       |100.69   |Snow   |2012|3    |1  |
; |2012-03-01 04:00:00|-5.4|-7.8          |83     |35      |1.6       |100.62   |Snow   |2012|3    |1  |
; +-------------------+----+--------------+-------+--------+----------+---------+-------+----+-----+---+
```

## 4.3 Getting the Temperature by Hour of Day

Extract the hour from the timestamp, group by it and calculate the mean temperature:

```clojure
(-> weather-mar-2012
    (g/with-column :hour (g/hour :date-time))
    (g/group-by :hour)
    (g/agg {:mean-temp (g/mean :temp)})
    (g/order-by :hour)
    (g/show {:num-rows 25}))
;; =stdout=>
; +----+-------------------+
; |hour|mean-temp          |
; +----+-------------------+
; |0   |2.074193548387097  |
; |1   |1.490322580645161  |
; |2   |0.9806451612903225 |
; |3   |0.49999999999999967|
; |4   |0.2806451612903219 |
; |5   |-0.1935483870967742|
; |6   |-0.1580645161290318|
; |7   |0.0806451612903229 |
; |8   |0.8548387096774198 |
; |9   |1.874193548387097  |
; |10  |2.8645161290322587 |
; |11  |3.9419354838709677 |
; |12  |5.0516129032258075 |
; |13  |5.9645161290322575 |
; |14  |6.519354838709678  |
; |15  |6.561290322580645  |
; |16  |6.812903225806451  |
; |17  |6.377419354838708  |
; |18  |5.254838709677419  |
; |19  |4.538709677419355  |
; |20  |4.025806451612903  |
; |21  |3.467741935483871  |
; |22  |3.1129032258064515 |
; |23  |2.63225806451613   |
; +----+-------------------+
```

`g/show` takes an optional map of options, including the number of rows to display.

## 4.4 Combining Monthly Data

To stack datasets vertically, use `g/union` when their columns have the same order, or `g/union-by-name` to match columns by name. Select matching columns before combining March and October:

```clojure
(def weather-oct-2012
  (g/select (weather-data 2012 10) (g/columns weather-mar-2012)))

(def weather-unioned (g/union weather-mar-2012 weather-oct-2012))

(g/count weather-unioned)
;; => 1488

(-> weather-unioned
    (g/group-by :year :month)
    g/count
    (g/order-by :year :month)
    g/show)
;; =stdout=>
; +----+-----+-----+
; |year|month|count|
; +----+-----+-----+
; |2012|3    |744  |
; |2012|10   |744  |
; +----+-----+-----+
```

## 4.5 Reading Multiple Files at Once

Spark writes a CSV dataset as a directory of part files. For this example, write one dataset per month. Overwrite mode makes the example safe to rerun:

```clojure
(doseq [month (range 1 13)]
  (g/write-csv! (weather-data 2012 month)
                (str "data/cookbook/weather-months/" month)
                {:mode "overwrite"}))
```

A glob reads all the monthly directories together:

```clojure
(def weather-2012
  (g/read-csv! "data/cookbook/weather-months/*"))

(-> weather-2012
    (g/group-by :year :month)
    g/count
    (g/order-by :year :month)
    g/show)
;; =stdout=>
; +----+-----+-----+
; |year|month|count|
; +----+-----+-----+
; |2012|1    |744  |
; |2012|2    |696  |
; |2012|3    |744  |
; |2012|4    |720  |
; |2012|5    |744  |
; |2012|6    |720  |
; |2012|7    |744  |
; |2012|8    |744  |
; |2012|9    |720  |
; |2012|10   |744  |
; |2012|11   |720  |
; |2012|12   |744  |
; +----+-----+-----+
```

Finally, save the combined dataset for part 5:

```clojure
(g/write-csv! weather-2012 "data/cookbook/weather-2012.csv" {:mode "overwrite"})
```
