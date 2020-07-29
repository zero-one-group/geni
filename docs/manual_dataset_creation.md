# Manual Dataset Creation

In order to manually create a instance of a Dataset, we must first create a Spark session. Typically, we can just do:

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
(g/to-df [[8 "bat"] [64 "mouse"] [-27 "horse"]]
         [:number :word])

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
