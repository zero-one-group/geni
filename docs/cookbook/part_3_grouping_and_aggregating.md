# CB3: Grouping and Aggregating

In this section, we reuse the Montréal cyclists data from part 1 of this cookbook:

```clojure
(def bikes
  (normalise-column-names
    (g/read-csv! spark bikes-data-path {:delimiter ";"
                                        :encoding "ISO-8859-1"
                                        :inferSchema "true"})))
```

## 3.1 Adding a Weekday Column

Firstly, we note that the `:date` column is parsed as string despite the `:inferSchema` option:

```clojure
(g/dtypes bikes)
=> {:brébeuf "StringType",
    :date "StringType",
    :du-parc "IntegerType",
    :côte-sainte-catherine "IntegerType",
    :maisonneuve-1 "IntegerType",
    :st-urbain "StringType",
    :rachel-1 "IntegerType",
    :pierre-dupuy "IntegerType",
    :berri-1 "IntegerType",
    :maisonneuve-2 "IntegerType"}
```

To parse the date column into the date type, we use `g/to-date` and specify the date format as `"dd/M/yyyy"`. We can use the function `g/with-column` to add (or, in this case, replace) the `:date` column with its new column value. Moreover, to calculate the day of the week, we use a similar trick by parsing the same date column with the weekday-only format `"EEEE"`:

```clojure
(def berri-bikes
  (-> bikes
      (g/with-column :date (g/to-date :date "dd/M/yyyy"))
      (g/with-column :weekday (g/date-format :date "EEEE"))
      (g/select :date :weekday :berri-1)))

(g/dtypes berri-bikes)
=> {:date "DateType", :weekday "StringType", :berri-1 "IntegerType"}

(g/show berri-bikes)
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
