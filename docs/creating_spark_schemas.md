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
(g/struct-type
 (g/struct-field :a :integer true)
 (g/struct-field :b :string true)
 (g/struct-field :c (g/array-type :short true) true)
 (g/struct-field :d (g/map-type :string :integer) true)
 (g/struct-field :e 
                 (g/struct-type 
                  (g/struct-field :x :float true) 
                  (g/struct-field :y :float true))
                 true))
```

whilst the Clojure version may look cleaner than the original Scala version, Geni offers an even more concise way to specify complex schemas such as the example above and cut through the boilerplates. In particular, we can use Geni's **data-oriented schemas**:

```clojure
{:a :long
 :b :string
 :c [:short]
 :d [:string :int]
 :z {:a :float :b :double}}
```

The conversion rules are simple:

* all fields and types default to nullable;
* a vector of count one is interpreted as an `ArrayType`;
* a vector of count two is interpreted as a `MapType`;
* a map is interpreted as a nested `StructType`; and
* everything else is left as is.

In particular, the last rule allows us to mix and match the data-oriented style with the Spark DataType style for specifying nested types.
