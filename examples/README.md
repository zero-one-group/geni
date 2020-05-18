# Geni Examples

The examples assume the following required namespaces:

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])
```

and a spark session, which can be defined as:

```clojure
(defonce spark (g/create-spark-session {}))
```

Example datasets can be found in the `test/resources` directory.

## Dataframe

The following examples are taken from [Apache Spark's example page](https://spark.apache.org/examples.html) and [Databricks' examples](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html).

### Text Search

```clojure
(-> melbourne-df
    (g/filter (g/like "Suburb" "%South%"))
    (g/select "Suburb")
    g/distinct
    g/show)

; Prints:
;
; +----------------+
; |Suburb          |
; +----------------+
; |South Melbourne |
; |South Kingsville|
; |Clayton South   |
; |Blackburn South |
; |Vermont South   |
; |Caulfield South |
; |Croydon South   |
; |Springvale South|
; |Melton South    |
; |Oakleigh South  |
; |Wantirna South  |
; |Southbank       |
; |South Morang    |
; |Frankston South |
; |South Yarra     |
; +----------------+
```

### Group-By and Aggregate

```clojure
(-> melbourne-df
    (g/group-by "Suburb")
    (g/agg (-> (g/count "*") (g/as "n")))
    (g/order-by (g/desc "n"))
    g/show)

; Prints:
;
; +--------------+---+
; |Suburb        |n  |
; +--------------+---+
; |Reservoir     |359|
; |Richmond      |260|
; |Bentleigh East|249|
; |Preston       |239|
; |Brunswick     |222|
; |Essendon      |220|
; |South Yarra   |202|
; |Glen Iris     |195|
; |Hawthorn      |191|
; |Coburg        |190|
; |Northcote     |188|
; |Brighton      |186|
; |Kew           |177|
; |Pascoe Vale   |171|
; |Balwyn North  |171|
; |Yarraville    |164|
; |St Kilda      |162|
; |Glenroy       |159|
; |Port Melbourne|153|
; |Moonee Ponds  |149|
; +--------------+---+
```

### Printing Schema

``` clojure
(-> melbourne-df
    g/print-schema)

; Prints:
;
; root
;  |-- Suburb: string (nullable = true)
;  |-- Address: string (nullable = true)
;  |-- Rooms: long (nullable = true)
;  |-- Type: string (nullable = true)
;  |-- Price: double (nullable = true)
;  |-- Method: string (nullable = true)
;  |-- SellerG: string (nullable = true)
;  |-- Date: string (nullable = true)
;  |-- Distance: double (nullable = true)
;  |-- Postcode: double (nullable = true)
;  |-- Bedroom2: double (nullable = true)
;  |-- Bathroom: double (nullable = true)
;  |-- Car: double (nullable = true)
;  |-- Landsize: double (nullable = true)
;  |-- BuildingArea: double (nullable = true)
;  |-- YearBuilt: double (nullable = true)
;  |-- CouncilArea: string (nullable = true)
;  |-- Lattitude: double (nullable = true)
;  |-- Longtitude: double (nullable = true)
;  |-- Regionname: string (nullable = true)
;  |-- Propertycount: double (nullable = true)
```

### Descriptive Statistics

```clojure
(-> melbourne-df
    (g/describe "Price")
    g/show)

; Prints:
;
; +-------+-----------------+
; |summary|Price            |
; +-------+-----------------+
; |count  |13580            |
; |mean   |1075684.079455081|
; |stddev |639310.7242960163|
; |min    |85000.0          |
; |max    |9000000.0        |
; +-------+-----------------+
```

### Null Rates

```clojure
(let [null-rate-fn   #(-> % g/null? (g/cast "int") g/mean (g/as %))
      null-rate-cols (map null-rate-fn (g/column-names melbourne-df))]
  (-> melbourne-df
      (g/agg null-rate-cols)
      g/show-vertical))

; Prints:
;
; -RECORD 0-----------------------------
;  Suburb        | 0.0
;  Address       | 0.0
;  Rooms         | 0.0
;  Type          | 0.0
;  Price         | 0.0
;  Method        | 0.0
;  SellerG       | 0.0
;  Date          | 0.0
;  Distance      | 0.0
;  Postcode      | 0.0
;  Bedroom2      | 0.0
;  Bathroom      | 0.0
;  Car           | 0.004565537555228277
;  Landsize      | 0.0
;  BuildingArea  | 0.47496318114874814
;  YearBuilt     | 0.3958026509572901
;  CouncilArea   | 0.1008100147275405
;  Lattitude     | 0.0
;  Longtitude    | 0.0
;  Regionname    | 0.0
;  Propertycount | 0.0
```

## MLlib

The following examples are taken from [Apache Spark's MLlib guide](https://spark.apache.org/docs/latest/ml-guide.html).

### Correlation

```clojure
(def corr-df
  (g/table->dataset
    spark
    [[[1.0 0.0 -2.0 0.0]]
     [[4.0 5.0 0.0  3.0]]
     [[6.0 7.0 0.0  8.0]]
     [[9.0 0.0 1.0  0.0]]]
    [:features]))

(let [corr-kw (keyword "pearson(features)")]
  (corr-kw (g/first (ml/corr corr-df "features"))))
; => ((1.0                  0.055641488407465814 0.9442673704375603  0.1311482458941057)
;     (0.055641488407465814 1.0                  0.22329687826943603 0.9428090415820635)
;     (0.9442673704375603   0.22329687826943603  1.0                 0.19298245614035084)
;     (0.1311482458941057   0.9428090415820635   0.19298245614035084 1.0))
```

### Hypothesis Testing

```clojure
(def hypothesis-df
  (g/table->dataset
     spark
     [[0.0 [0.5 10.0]]
      [0.0 [1.5 20.0]]
      [1.0 [1.5 30.0]]
      [0.0 [3.5 30.0]]
      [0.0 [3.5 40.0]]
      [1.0 [3.5 40.0]]]
     [:label :features]))

(g/first (ml/chi-square-test hypothesis-df "features" "label"))
; => {:pValues (0.6872892787909721 0.6822703303362126),
;     :degreesOfFreedom (2 3),
;     :statistics (0.75 1.5))
```
