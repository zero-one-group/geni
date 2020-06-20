## Geni Semantics

### Column Coercion

Many SQL functions and Column methods are overloaded to take either a keyword, a string or a Column instance as argument. For such cases, Geni implements Column coercion where

1. Column instances are left as they are,
2. strings and keywords are interpreted as column names and;
3. other values are interpreted as a literal Column.

Because of this, basic arithmetic operations do not require `lit` wrapping:

```clojure
; The following two expressions are equivalent
(g/- (g// (g/sin Math/PI) (g/cos Math/PI)) (g/tan Math/PI))
(g/- (g// (g/sin (g/lit Math/PI)) (g/cos (g/lit Math/PI))) (g/tan (g/lit Math/PI)))
```

However, string literals do require `lit` wrapping:

```clojure
; The following fails, because "Nelson" is interpreted as a Column
(-> dataframe (g/filter (g/=== "SellerG" "Nelson")))

; The following works, as it checks the column "SellerG" against "Nelson" as a literal
(-> dataframe (g/filter (g/=== "SellerG" (g/lit "Nelson"))))
```

It may be useful to think of a Spark Dataset as a seq of maps, so that keywords can be idiomatically used to refer to columns (i.e. keys). For that reason, the predicate column above may be more idiomatically written as:

```clojure
(g/=== :SellerG (g/lit "Nelson"))
```

### Column-Array Coercion

Geni implements Column-array coercion to variadic SQL functions and Column methods, such as `select` and `group-by`. The coercion rules are as follos:

1. maps have their values flattened, coerced into Columns and aliased as the keys and;
2. other collections have their values flattened and coerced into Columns.
3. otherwise the argument is directly coerced into  Column.

A function like `select` can take all of these different types in a single invocation:

```clojure
(-> dataframe
    (g/select :SellerG
              "Address"
              (g/col "Postcode")
              {:log-price (g/log :Price) :rooms :Rooms}
              [:Date :Method]
              #{:Lattitude :Longtitude})
    g/columns)
=> (:SellerG :Address :Postcode :log-price :rooms :Date :Method :Lattitude :Longtitude)
```

### Boolean Casts

All calls to `filter` and `remove` are implicitly casted to booleans. This means that the Columns can be left as, say, integers:

```clojure
(-> dataframe
    (g/remove (g/mod :Rooms 2))
    (g/select :Rooms)
    g/distinct
    (g/collect-col :Rooms))
=> (4 6 2 10 8)
```

### ArrayType vs. VectorType in Dataset Creation

Inspired by Pandas' flexible DataFrame creation, Geni provides three main ways to create Spark Datasets:

```clojure
; The following three expressions are equivalent
(g/table->dataset spark
                  [[1 "x"]
                   [2 "y"]
                   [3 "z"]]
                  [:a :b])
(g/map->dataset spark {:a [1 2 3] :b ["x" "y" "z"]})
(g/records->dataset spark [{:a 1 :b "x"}
                           {:a 2 :b "y"}
                           {:a 3 :b "z"}])
```

It it sometimes convenient to be able to create a Spark vector column, which is different to SQL array columns. For that reason, Geni provides an easy way to create vector columns, but it comes with a potential gotcha. A vector of numbers is interpreted as a Spark vector, but any list is always interpreted as a SQL array:

```clojure
(g/print-schema
  (g/table->dataset spark
                    [[0.0 [0.5 10.0]]
                     [1.0 [1.5 30.0]]]
                    [:label :features]))
; root
;  |-- label: double (nullable = true)
;  |-- features: vector (nullable = true)

(g/print-schema
  (g/table->dataset spark
                    [[0.0 '(0.5 10.0)]
                     [1.0 '(1.5 30.0)]]
                    [:label :features]))
; root
;  |-- label: double (nullable = true)
;  |-- features: array (nullable = true)
;  |    |-- element: double (containsNull = true)
```
