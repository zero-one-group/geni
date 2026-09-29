# Creating Spark Schemas

Schema creation is typically required for [manual Dataset creation](manual_dataset_creation.md) and for having more control when loading a Dataset from file.

One way to create a Spark schema is to use the Geni API that closely mimics the original Scala Spark API using **Spark DataTypes**. That is, the following Scala version:

```scala
StructType(Array(
    StructField("a", IntegerType, true),
    StructField("b", StringType, true),
    StructField("c", ArrayType(ShortType, true), true),
    StructField("d", MapType(StringType, IntegerType, true), true),
    StructField(
        "e",
        StructType(Array(
            StructField("x", FloatType, true),
            StructField("y", DoubleType, true)
        )),
        true
    )
))
```

gets translated into:

```clojure
(require '[zero-one.geni.core :as g])

(def spark-style-schema
  (g/struct-type
   (g/struct-field :a :int true)
   (g/struct-field :b :str true)
   (g/struct-field :c (g/array-type :short true) true)
   (g/struct-field :d (g/map-type :str :int) true)
   (g/struct-field :e
                   (g/struct-type
                    (g/struct-field :x :float true)
                    (g/struct-field :y :double true))
                   true)))
```

Whilst the Clojure version may look cleaner than the original Scala version, Geni offers an even more concise way to specify complex schemas such as the example above and cut through the boilerplate. In particular, Geni's **data-oriented schemas** describe the same schema as:

```clojure
(def data-oriented-schema
  {:a :int
   :b :str
   :c [:short]
   :d [:str :int]
   :e {:x :float :y :double}})

(= spark-style-schema (g/->schema data-oriented-schema))
;; => true
```

Functions that take a schema, such as `g/create-dataframe`, take either kind. The conversion rules are simple:

* all fields and types default to nullable;
* a vector of count one is interpreted as an `ArrayType`;
* a vector of count two is interpreted as a `MapType`;
* a map is interpreted as a nested `StructType`; and
* everything else is left as is.

In particular, the last rule allows mixing and matching the data-oriented style with the Spark DataType style for specifying nested types.
