# Manual Dataset Creation

The examples below only need Geni's core namespace. Geni starts a Spark session the first time a function needs one (see [Where's the Spark session?](spark_session.md)):

```clojure
(require '[zero-one.geni.core :as g])
```

In a production setting, we typically would not be manually instantiating our own Spark Dataset. However, it can be useful for testing and example purposes. Geni provides two main ways to do this, namely the usual [Spark way](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393) and a couple of shortcuts inspired by [Pandas dataframe creation](https://www.geeksforgeeks.org/different-ways-to-create-pandas-dataframe/).

We will be using the example used by [Matthew Powers' blog post](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393). In native Scala:

```scala
// using toDF
import spark.implicits._

val someDF = Seq(
  (8, "bat"),
  (64, "mouse"),
  (-27, "horse")
).toDF("number", "word")

// using Rows and Schema
val someData = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val someSchema = List(
  StructField("number", IntegerType, true),
  StructField("word", StringType, true)
)

val someDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)
```

## Verbatim Translation

In Geni, the above Scala codes would translate to the following respectively:

```clojure
(-> (g/to-df [[8 "bat"] [64 "mouse"] [-27 "horse"]]
             [:number :word])
    g/show)
;; =stdout=>
; +------+-----+
; |number|word |
; +------+-----+
; |8     |bat  |
; |64    |mouse|
; |-27   |horse|
; +------+-----+

(g/create-dataframe [(g/row 8 "bat")
                     (g/row 64 "mouse")
                     (g/row -27 "horse")]
                    (g/struct-type
                      (g/struct-field :number :long true)
                      (g/struct-field :word :string true)))
```

## Shortcuts

In Pandas, we could create DataFrames using a nested list (or a table). Geni provides `table->dataset`, which coincidentally is identical to `to-df`:

```clojure
(g/table->dataset [[8 "bat"] [64 "mouse"] [-27 "horse"]]
                  [:number :word])
```

The second method is to use a dictionary (i.e. map) of column name to column values:

```clojure
(g/map->dataset {:number [8 64 -27]
                 :word   ["bat" "mouse" "horse"]})
```

The third and final method is to use a list of dictionaries with fixed keys (i.e. a seq of records):

```clojure
(g/records->dataset [{:number   8 :word "bat"}
                     {:number  64 :word "mouse"}
                     {:number -27 :word "horse"}])
```

## Inferred Types

The three shortcuts infer each column's type from its first value that isn't nil:

| Value | Spark type |
|---|---|
| a boolean, a number such as `1` or `1.0`, or a string | the matching type, such as `LongType` for `1` |
| a `BigDecimal`, such as `1.5M` | `DecimalType(38,18)`, Spark's default decimal |
| a `BigInt` or a `BigInteger`, such as `1N` | `DecimalType(38,0)` |
| a `java.time.LocalDate` or a `java.sql.Date` | `DateType` |
| a `java.time.Instant`, a `java.sql.Timestamp` or a `java.util.Date`, such as `#inst "2026-10-01"` | `TimestampType` |
| a `java.time.LocalDateTime` | `TimestampNTZType` |
| a keyword or a `java.util.UUID` | `StringType`, with `"geni/new"` for `:geni/new` |
| a byte array | `BinaryType` |
| a map | a struct of the map's keys |
| a vector or a list | an array |

```clojure
(-> (g/records->dataset [{:price 1.5M
                          :day   (java.time.LocalDate/of 2026 10 1)
                          :tag   :geni/new}])
    g/dtypes)
;; => {:price "DecimalType(38,18)", :day "DateType", :tag "StringType"}
```

A value of any other class, such as the ratio `1/3`, throws an error that names its column, so convert it first.
