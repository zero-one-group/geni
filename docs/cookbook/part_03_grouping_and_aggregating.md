# CB-03: Grouping and Aggregating

In this section, we reuse the Montréal cyclists data from part 1 of this cookbook. As in every part, we start with Geni and the `download-data!` function from [part 1](part_01_reading_and_writing_datasets.md):

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

Then we download the data, unless part 1 already did, and load it:

```clojure
(def bikes-data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/80817f5a46ef6d2f8444eeb7181e00c0aabb5a57/cookbook/data/bikes.csv")

(def bikes-data-path "data/cookbook/bikes.csv")

(download-data! bikes-data-url bikes-data-path)

(def bikes
  (g/read-csv! bikes-data-path {:delimiter ";"
                                :encoding "ISO-8859-1"
                                :kebab-columns true}))
```

## 3.1 Adding a Weekday Column

Firstly, we note that the `:date` column is parsed as string:

```clojure
(g/dtypes bikes)
;; => {:date "StringType",
;;     :st-urbain-donnees-non-disponibles "StringType",
;;     :du-parc "IntegerType",
;;     :maisonneuve-1 "IntegerType",
;;     :rachel-1 "IntegerType",
;;     :pierre-dupuy "IntegerType",
;;     :berri-1 "IntegerType",
;;     :cote-sainte-catherine "IntegerType",
;;     :brebeuf-donnees-non-disponibles "StringType",
;;     :maisonneuve-2 "IntegerType"}
```

To parse the date column into the date type, we use `g/to-date` and specify the date format as `"dd/M/yyyy"`. We can use the function `g/with-column` to add (or, in this case, replace) the `:date` column with its new column value. Moreover, to calculate the day of the week, we use a similar trick by parsing the same date column with the weekday-only format `"EEEE"`:

```clojure
(def berri-bikes
  (-> bikes
      (g/with-column :date (g/to-date :date "dd/M/yyyy"))
      (g/with-column :weekday (g/date-format :date "EEEE"))
      (g/select :date :weekday :berri-1)))

(g/dtypes berri-bikes)
;; => {:date "DateType", :weekday "StringType", :berri-1 "IntegerType"}

(g/show berri-bikes)
;; =stdout=>
; +----------+---------+-------+
; |date      |weekday  |berri-1|
; +----------+---------+-------+
; |2012-01-01|Sunday   |35     |
; |2012-01-02|Monday   |83     |
; |2012-01-03|Tuesday  |135    |
; |2012-01-04|Wednesday|144    |
; |2012-01-05|Thursday |197    |
; |2012-01-06|Friday   |146    |
; |2012-01-07|Saturday |98     |
; |2012-01-08|Sunday   |95     |
; |2012-01-09|Monday   |244    |
; |2012-01-10|Tuesday  |397    |
; |2012-01-11|Wednesday|273    |
; |2012-01-12|Thursday |157    |
; |2012-01-13|Friday   |75     |
; |2012-01-14|Saturday |32     |
; |2012-01-15|Sunday   |54     |
; |2012-01-16|Monday   |168    |
; |2012-01-17|Tuesday  |155    |
; |2012-01-18|Wednesday|139    |
; |2012-01-19|Thursday |191    |
; |2012-01-20|Friday   |161    |
; +----------+---------+-------+
; only showing top 20 rows
```

## 3.2 Adding Up The Cyclists By Weekday

To add up the cyclists by weekday, we compose `g/group-by` with `g/sum`:

```clojure
(-> berri-bikes
    (g/group-by :weekday)
    (g/sum :berri-1)
    g/show)
;; =stdout=>
; +---------+------------+
; |weekday  |sum(berri-1)|
; +---------+------------+
; |Wednesday|152972      |
; |Tuesday  |135305      |
; |Friday   |141771      |
; |Thursday |160131      |
; |Saturday |101578      |
; |Monday   |134298      |
; |Sunday   |99310       |
; +---------+------------+
```

We may not like the default `sum(berri-1)`. We can rename it using `g/select` in part 1 or, alternatively, we can do an aggregate using `g/agg` with a rename map:

```clojure
(-> berri-bikes
    (g/group-by :weekday)
    (g/agg {:n-cyclists (g/sum :berri-1)})
    g/show)
;; =stdout=>
; +---------+----------+
; |weekday  |n-cyclists|
; +---------+----------+
; |Wednesday|152972    |
; |Tuesday  |135305    |
; |Friday   |141771    |
; |Thursday |160131    |
; |Saturday |101578    |
; |Monday   |134298    |
; |Sunday   |99310     |
; +---------+----------+
```
