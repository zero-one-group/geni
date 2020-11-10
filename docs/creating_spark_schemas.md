# Creating Spark Schemas

Schema creation is typically required for manual Dataset creation and for having more control when loading a Dataset from file.

One way to create a Spark schema is to use the Geni API that closely mimics the original Scala Spark API. That is, using **Spark DataTypes**, the following Scala version:

```scala

val someSchema = StructType(Array(
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
(g/struct-type
  (g/struct-field :a :integer true)
  (g/struct-field :b :string true)
  (g/struct-field :c (g/array-type :short true) true)
  (g/struct-field :d (g/map-type :string :integer) true)
  (g/struct-field :e 
                  (g/struct-type (g/struct-field :x :float true) 
                                 (g/struct-field :y :float true))
                  true))
```

whilst the Clojure version may look cleaner than the original Scala version, Geni offers an even cleaner way of specifying complex schemas such as the example above.

Geni's **data-oriented schemas** cut through the boilerplates:

```clojure
{:a :long
 :b :string
 :c [:short]
 :d [:string :int]
 :z {:a :float :b :double}}
```

The conversion rules are simple:

* All fields and types default to nullable.
* A vector of count one is interpreted as an `ArrayType`.
* A vector of count two is interpreted as a `MapType`.
* A map is interpreted as a nested `StructType`.
* Everything else is left as is.

The last rule allows us to mix and match the data-oriented style with the Spark DataType style for specifying nested types.
