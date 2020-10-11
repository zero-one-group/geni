# CB-04: Combining Datasets with Joins and Unions

As in the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%205%20-%20Combining%20dataframes%20and%20scraping%20Canadian%20weather%20data.ipynb), we are going to use the Canadian historical weather data, which is freely available [here](https://climate.weather.gc.ca/). 

## 4.1 Downloading One Month of Data

We can download the data by hitting the right URL query. We can programmatically setup the query and load the data as follows:

```clojure
(defn weather-data-url [year month]
  (str "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
       "format=csv&stationID=5415&Year=" year "&Month=" month
       "&timeframe=1&submit=Download+Data"))

(defn weather-data-path [year month]
  (str "data/cookbook/weather/weather-" year "-" month ".csv"))

(defn weather-data [year month]
  (download-data! (weather-data-url year month) (weather-data-path year month))
  (g/read-csv! (weather-data-path year month))))

(def raw-weather-mar-2012 (weather-data 2012 3))

(g/count raw-weather-mar-2012)
=> 744

(g/print-schema raw-weather-mar-2012)
; root
;  |-- longitude-x: double (nullable = true)
;  |-- latitude-y: double (nullable = true)
;  |-- station-name: string (nullable = true)
;  |-- climate-id: integer (nullable = true)
;  |-- date-time: string (nullable = true)
;  |-- year: integer (nullable = true)
;  |-- month: integer (nullable = true)
;  |-- day: integer (nullable = true)
;  |-- time: string (nullable = true)
;  |-- temp-c: double (nullable = true)
;  |-- temp-flag: string (nullable = true)
;  |-- dew-point-temp-c: double (nullable = true)
;  |-- dew-point-temp-flag: string (nullable = true)
;  |-- rel-hum: integer (nullable = true)
;  |-- rel-hum-flag: string (nullable = true)
;  |-- wind-dir-10s-deg: integer (nullable = true)
;  |-- wind-dir-flag: string (nullable = true)
;  |-- wind-spd-kmh: integer (nullable = true)
;  |-- wind-spd-flag: string (nullable = true)
;  |-- visibility-km: double (nullable = true)
;  |-- visibility-flag: string (nullable = true)
;  |-- stn-press-k-pa: double (nullable = true)
;  |-- stn-press-flag: string (nullable = true)
;  |-- hmdx: integer (nullable = true)
;  |-- hmdx-flag: string (nullable = true)
;  |-- wind-chill: integer (nullable = true)
;  |-- wind-chill-flag: string (nullable = true)
;  |-- weather: string (nullable = true)
```

## 4.2 Dropping Columns with Nulls

Looking through the data, we notice that some columns contain mostly nulls:

```clojure
(g/show raw-weather-mar-2012)
; +---------+--------+--------------------------------------+----------+----------------+----+-----+---+-----+----+---------+--------------+-------------------+-------+------------+--------+-------------+--------+-------------+----------+---------------+---------+--------------+----+---------+----------+---------------+-------+
; |longitude|latitude|station-name                          |climate-id|date-time       |year|month|day|time |temp|temp-flag|dew-point-temp|dew-point-temp-flag|rel-hum|rel-hum-flag|wind-dir|wind-dir-flag|wind-spd|wind-spd-flag|visibility|visibility-flag|stn-press|stn-press-flag|hmdx|hmdx-flag|wind-chill|wind-chill-flag|weather|
; +---------+--------+--------------------------------------+----------+----------------+----+-----+---+-----+----+---------+--------------+-------------------+-------+------------+--------+-------------+--------+-------------+----------+---------------+---------+--------------+----+---------+----------+---------------+-------+
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 00:00|2012|3    |1  |00:00|-5.5|null     |-9.7          |null               |72     |null        |5       |null         |24      |null         |4.0       |null           |100.97   |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 01:00|2012|3    |1  |01:00|-5.7|null     |-8.7          |null               |79     |null        |6       |null         |26      |null         |2.4       |null           |100.87   |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 02:00|2012|3    |1  |02:00|-5.4|null     |-8.3          |null               |80     |null        |5       |null         |28      |null         |4.8       |null           |100.8    |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 03:00|2012|3    |1  |03:00|-4.7|null     |-7.7          |null               |79     |null        |5       |null         |28      |null         |4.0       |null           |100.69   |null          |null|null     |-12       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 04:00|2012|3    |1  |04:00|-5.4|null     |-7.8          |null               |83     |null        |5       |null         |35      |null         |1.6       |null           |100.62   |null          |null|null     |-14       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 05:00|2012|3    |1  |05:00|-5.3|null     |-7.9          |null               |82     |null        |4       |null         |33      |null         |2.4       |null           |100.58   |null          |null|null     |-14       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 06:00|2012|3    |1  |06:00|-5.2|null     |-7.8          |null               |82     |null        |5       |null         |33      |null         |4.0       |null           |100.57   |null          |null|null     |-14       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 07:00|2012|3    |1  |07:00|-4.9|null     |-7.4          |null               |83     |null        |5       |null         |30      |null         |1.6       |null           |100.59   |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 08:00|2012|3    |1  |08:00|-5.0|null     |-7.5          |null               |83     |null        |5       |null         |32      |null         |1.2       |null           |100.59   |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 09:00|2012|3    |1  |09:00|-4.9|null     |-7.5          |null               |82     |null        |5       |null         |32      |null         |1.6       |null           |100.6    |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 10:00|2012|3    |1  |10:00|-4.7|null     |-7.3          |null               |82     |null        |5       |null         |32      |null         |1.2       |null           |100.62   |null          |null|null     |-13       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 11:00|2012|3    |1  |11:00|-4.4|null     |-6.8          |null               |83     |null        |5       |null         |28      |null         |1.0       |null           |100.66   |null          |null|null     |-12       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 12:00|2012|3    |1  |12:00|-4.3|null     |-6.8          |null               |83     |null        |5       |null         |30      |null         |1.2       |null           |100.66   |null          |null|null     |-12       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 13:00|2012|3    |1  |13:00|-4.3|null     |-6.9          |null               |82     |null        |6       |null         |28      |null         |1.2       |null           |100.65   |null          |null|null     |-12       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 14:00|2012|3    |1  |14:00|-3.9|null     |-6.6          |null               |81     |null        |6       |null         |28      |null         |1.2       |null           |100.67   |null          |null|null     |-11       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 15:00|2012|3    |1  |15:00|-3.3|null     |-6.2          |null               |80     |null        |6       |null         |24      |null         |1.6       |null           |100.71   |null          |null|null     |-10       |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 16:00|2012|3    |1  |16:00|-2.7|null     |-5.7          |null               |80     |null        |7       |null         |19      |null         |2.4       |null           |100.74   |null          |null|null     |-8        |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 17:00|2012|3    |1  |17:00|-2.9|null     |-5.9          |null               |80     |null        |5       |null         |20      |null         |4.0       |null           |100.8    |null          |null|null     |-9        |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 18:00|2012|3    |1  |18:00|-3.0|null     |-6.0          |null               |80     |null        |6       |null         |19      |null         |4.0       |null           |100.87   |null          |null|null     |-9        |null           |Snow   |
; |-73.75   |45.47   |MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A|7025250   |2012-03-01 19:00|2012|3    |1  |19:00|-3.6|null     |-6.4          |null               |81     |null        |6       |null         |17      |null         |3.2       |null           |100.93   |null          |null|null     |-9        |null           |Snow   |
; +---------+--------+--------------------------------------+----------+----------------+----+-----+---+-----+----+---------+--------------+-------------------+-------+------------+--------+-------------+--------+-------------+----------+---------------+---------+--------------+----+---------+----------+---------------+-------+
; only showing top 20 rows
```

To find out the null counts of each column, we can do an aggregate operation using `g/agg` and calculate the counts using `g/null-count`:

```clojure
(def null-counts
  (-> raw-weather-mar-2012
      (g/agg (->> (g/column-names raw-weather-mar-2012)
                  (map #(vector % (g/null-count %)))
                  (into {})))
      g/first))

null-counts
=> {:wind-dir 29,
    :rel-hum-flag 744,
    :day 0,
    :temp 0,
    :wind-spd 0,
    :date-time 0,
    :stn-press 0,
    :visibility-flag 744,
    :wind-chill-flag 743,
    :time 0,
    :temp-flag 744,
    :climate-id 0,
    :month 0,
    :longitude 0,
    :dew-point-temp 0,
    :wind-chill 502,
    :dew-point-temp-flag 744,
    :hmdx-flag 744,
    :year 0,
    :hmdx 732,
    :stn-press-flag 744,
    :wind-spd-flag 741,
    :rel-hum 0,
    :latitude 0,
    :weather 0,
    :station-name 0,
    :visibility 0,
    :wind-dir-flag 744}
```

Next, grab the columns that have no nulls and preserve the order of the original columns:

```clojure
(def columns-without-nulls
  (->> null-counts
       (filter #(zero? (second %)))
       (map first)))

(def columns-to-select
  (filter (set columns-without-nulls) (g/columns raw-weather-mar-2012)))
=> (:longitude
    :latitude
    :station-name
    :climate-id
    :date-time
    :year
    :month
    :day
    :time
    :temp
    :dew-point-temp
    :rel-hum
    :wind-spd
    :visibility
    :stn-press
    :weather)
```

We may also drop other irrelevant columns:

```clojure
(def weather-mar-2012
  (-> raw-weather-mar-2012
      (g/select columns-to-select)
      (g/drop :longitude
              :latitude
              :station-name
              :climate-id
              :time
              :data-quality)))

(g/show weather-mar-2012)
; +----------------+----+-----+---+----+--------------+-------+--------+----------+---------+-------+
; |date-time       |year|month|day|temp|dew-point-temp|rel-hum|wind-spd|visibility|stn-press|weather|
; +----------------+----+-----+---+----+--------------+-------+--------+----------+---------+-------+
; |2012-03-01 00:00|2012|3    |1  |-5.5|-9.7          |72     |24      |4.0       |100.97   |Snow   |
; |2012-03-01 01:00|2012|3    |1  |-5.7|-8.7          |79     |26      |2.4       |100.87   |Snow   |
; |2012-03-01 02:00|2012|3    |1  |-5.4|-8.3          |80     |28      |4.8       |100.8    |Snow   |
; |2012-03-01 03:00|2012|3    |1  |-4.7|-7.7          |79     |28      |4.0       |100.69   |Snow   |
; |2012-03-01 04:00|2012|3    |1  |-5.4|-7.8          |83     |35      |1.6       |100.62   |Snow   |
; |2012-03-01 05:00|2012|3    |1  |-5.3|-7.9          |82     |33      |2.4       |100.58   |Snow   |
; |2012-03-01 06:00|2012|3    |1  |-5.2|-7.8          |82     |33      |4.0       |100.57   |Snow   |
; |2012-03-01 07:00|2012|3    |1  |-4.9|-7.4          |83     |30      |1.6       |100.59   |Snow   |
; |2012-03-01 08:00|2012|3    |1  |-5.0|-7.5          |83     |32      |1.2       |100.59   |Snow   |
; |2012-03-01 09:00|2012|3    |1  |-4.9|-7.5          |82     |32      |1.6       |100.6    |Snow   |
; |2012-03-01 10:00|2012|3    |1  |-4.7|-7.3          |82     |32      |1.2       |100.62   |Snow   |
; |2012-03-01 11:00|2012|3    |1  |-4.4|-6.8          |83     |28      |1.0       |100.66   |Snow   |
; |2012-03-01 12:00|2012|3    |1  |-4.3|-6.8          |83     |30      |1.2       |100.66   |Snow   |
; |2012-03-01 13:00|2012|3    |1  |-4.3|-6.9          |82     |28      |1.2       |100.65   |Snow   |
; |2012-03-01 14:00|2012|3    |1  |-3.9|-6.6          |81     |28      |1.2       |100.67   |Snow   |
; |2012-03-01 15:00|2012|3    |1  |-3.3|-6.2          |80     |24      |1.6       |100.71   |Snow   |
; |2012-03-01 16:00|2012|3    |1  |-2.7|-5.7          |80     |19      |2.4       |100.74   |Snow   |
; |2012-03-01 17:00|2012|3    |1  |-2.9|-5.9          |80     |20      |4.0       |100.8    |Snow   |
; |2012-03-01 18:00|2012|3    |1  |-3.0|-6.0          |80     |19      |4.0       |100.87   |Snow   |
; |2012-03-01 19:00|2012|3    |1  |-3.6|-6.4          |81     |17      |3.2       |100.93   |Snow   |
; +----------------+----+-----+---+----+--------------+-------+--------+----------+---------+-------+
; only showing top 20 rows
```

## 4.3 Getting the Temperature by Hour of Day

To extract the hour of day, we must first create a timestamp column using `g/to-timestamp` and extract the hour by invoking `g/hour`. The rest is simply another group-by + aggregate operations:

```clojure
(-> weather-mar-2012
    (g/with-column :hour (g/hour (g/to-timestamp :date-time "yyyy-M-d HH:mm")))
    (g/group-by :hour)
    (g/agg {:mean-temp (g/mean :temp-c)})
    (g/order-by :hour)
    (g/show {:num-rows 25}))
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
 Notice that `g/show` takes an optional map of options, where we can specify the number of rows.

## 4.4 Combining Monthly Data

To vertically stack datasets, we use `g/union` and `g/union-by-name`. The former is used when all the columns line up, and the latter is used when the order of the columns are not necessarily in the same order. In this case, we can do a select operation first and use `g/union`:

```clojure
(def weather-oct-2012
  (-> (weather-data 2012 10)
      (g/select (g/columns weather-mar-2012))))

(def weather-unioned
  (g/union weather-mar-2012 weather-oct-2012))
```

To verify that the vertical stacking (or union) operation is successful, we check that both months are present:

```clojure
(g/count weather-unioned)
=> 1488

(-> weather-unioned
    (g/select :year :month)
    g/value-counts
    g/show)
; +----+-----+-----+
; |year|month|count|
; +----+-----+-----+
; |2012|10   |744  |
; |2012|3    |744  |
; +----+-----+-----+
```

## 4.5 Reading Multiple Files at Once

The function `g/read-csv!` can actually read a directory containing multiple CSV files. For instance, we may download all 2012 monthly data:

```clojure
(mapv (partial weather-data 2012) (range 1 13))
```

and we can simply set the CSV path to the directory path:

```clojure
(def weather-2012
  (-> (g/read-csv! "data/cookbook/weather" {:kebab-columns true})
      (g/select (g/columns weather-mar-2012))))

(-> weather-2012
    (g/group-by :year :month)
    g/count
    (g/order-by :year :month)
    g/show)
; +----+-----+-----+
; |year|month|count|
; +----+-----+-----+
; |2012|01   |744  |
; |2012|02   |696  |
; |2012|03   |744  |
; |2012|04   |720  |
; |2012|05   |744  |
; |2012|06   |720  |
; |2012|07   |744  |
; |2012|08   |744  |
; |2012|09   |720  |
; |2012|10   |744  |
; |2012|11   |720  |
; |2012|12   |744  |
; +----+-----+-----+
```

Finally, we can save the aggregated dataset for future use:

```clojure
(g/write-csv! weather-2012 "data/cookbook/weather-2012.csv")
```
