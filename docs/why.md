## Why?

Many data tasks such as exploratory data analysis require frequent and rapid feedback from the data. Geni optimises for the speed of this feedback loop by providing a dynamic and terse interface that complements [REPL-driven development](https://vimeo.com/223309989) or [conversational software development](https://oli.me.uk/conversational-software-development/).

The examples below use the Melbourne housing data in Geni's repo:

```clojure
(require '[zero-one.geni.core :as g])

(def dataframe (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet"))
```

For many Geni functions, the types don't have to line up; the args only have to be convertible into Spark Columns. Consider the following example:

```clojure
(-> dataframe
    (g/group-by (g/lower "SellerG")  ;; Mixed Column, string and keyword types.
                "Suburb"             ;; No need for `into-array`.
                :Regionname)
    (g/agg {:mean (g/mean :Price)    ;; Map keys are interpreted as aliases.
            :std  (g/stddev :Price)
            :min  (g/min :Price)
            :max  (g/max :Price)})
    g/show)
```

With pure interop, the same query reads:

<!-- :test-doc-blocks/skip -->
```clojure
(import '(org.apache.spark.sql functions Column))

(-> dataframe
    (.groupBy (into-array Column [(functions/lower (functions/col "SellerG"))
                                  (functions/col "Suburb")
                                  (functions/col "Regionname")]))
    (.agg
      (.as (functions/mean "Price") "mean")
      (into-array Column [(.as (functions/stddev "Price") "std")
                          (.as (functions/min "Price") "min")
                          (.as (functions/max "Price") "max")]))
    .show)
```

At times, it can be tricky to figure out the interop, which often requires careful inspection of Java reflection. This problem is compounded in the case of Scala interop:

```clojure
(import '(scala.collection JavaConverters))

(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map #(JavaConverters/seqAsJavaList (.toSeq %))))
     ;; returns a seq of lists, which still need zipping with the column names
```

Geni handles all the interop in the background: `(g/collect dataframe)` returns a seq of maps, where the keys are keywordised and nested structs are collected as nested maps. Collecting deeply nested structs as maps becomes straightforward:

```clojure
(-> dataframe
    (g/select
      {:property
       (g/struct
         {:market   (g/struct :SellerG :Price :Date)
          :house    (g/struct :Landsize :Rooms)
          :location (g/struct :Address {:coord (g/struct :Lattitude :Longtitude)})})})
    (g/limit 1)
    g/collect)
;; => ({:property
;;      {:market {:SellerG "Biggin", :Price 1480000.0, :Date "3/12/2016"},
;;       :house {:Landsize 202.0, :Rooms 2},
;;       :location
;;       {:Address "85 Turner St",
;;        :coord {:Lattitude -37.7996, :Longtitude 144.9984}}}})
```

Finally, Geni supports various Clojure (or Lisp) idioms by making some functions variadic (`+`, `<=`, `&&`, etc.) and providing functions with Clojure analogues that are not available in Spark such as `remove`. For example:

```clojure
(-> dataframe
    (g/remove (g/like :Regionname "%Metropolitan%"))
    (g/filter (g/&& (g/< 2 :Rooms 5)
                    (g/< 5e5 :Price 6e5)
                    (g/< :YearBuilt 2010)))
    (g/select :Regionname :Rooms :Price :YearBuilt)
    g/show)
;; =stdout=>
; +-----------------+-----+--------+---------+
; |Regionname       |Rooms|Price   |YearBuilt|
; +-----------------+-----+--------+---------+
; |Northern Victoria|4    |521000.0|1980.0   |
; |Northern Victoria|3    |540000.0|1930.0   |
; |Eastern Victoria |3    |581000.0|1970.0   |
; |Western Victoria |4    |550000.0|1970.0   |
; |Eastern Victoria |3    |570000.0|1960.0   |
; |Eastern Victoria |3    |515000.0|1970.0   |
; +-----------------+-----+--------+---------+
```

Note that functions such as `g/remove` and `g/filter` take the Spark Dataset as their first argument. This departure from Clojure's idioms is what lets the threading macro `->` stand in for Scala's method chaining.
