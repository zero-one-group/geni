## Why?

Many data tasks such as exploratory data analysis require frequent and rapid feedback from the data. Geni optimises for the speed of this feedback loop by providing a dynamic and terse interface that complements [REPL-driven development](https://vimeo.com/223309989) or [conversational software development](https://oli.me.uk/conversational-software-development/).

For many Geni functions, we do not need to make sure that the types line up; only that the args can be converted into Spark Columns. Consider the following example:

```clojure
(-> dataframe
    (group-by (lower "SellerG")  ;; Mixed Column, string and keyword types.
              "Suburb"           ;; No need for `into-array`.
              :Regionname)
    (agg {:mean (mean :Price)    ;; Map keys are interpreted as aliases.
          :std  (stddev :Price)
          :min  (min :Price)
          :max  (max :Price)})
    show)
```

In contrast, we would have to write the following with pure interop:

```clojure
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

At times, it can be tricky to figure out the interop , which often times requires careful inspection of Java reflection. This problem is compounded in the case of Scala interop:

```clojure
(import '(scala.collection JavaConversions))

(->> (.collect dataframe) ;; .collect returns an array of Spark rows
     (map #(JavaConversions/seqAsJavaList (.. % toSeq))))
     ;; returns a seq of seqs - must zipmap with col names to get maps
```

Geni handles all the interop in the background - `(collect dataframe)` returns a seq of maps, where the keys are keywordised and nested structs are collected as nested maps. Collecting deeply nested structs as maps becomes straightforward:

```clojure
(-> dataframe
    (select
      {:property
       (struct
         {:market   (struct :SellerG :Price :Date)
          :house    (struct :Landsize :Rooms)
          :location (struct :Address {:coord (struct :Lattitude :Longtitude)})})})
    (limit 1)
    collect)
=> ({:property
     {:market {:SellerG "Biggin", :Price 1480000.0, :Date "3/12/2016"},
      :house {:Landsize 202.0, :Rooms 2},
      :location
      {:Suburb "Abbotsford",
       :Address "85 Turner St",
       :coord {:Lattitude -37.7996, :Longtitude 144.9984}}}})
```

Finally, Geni supports various Clojure (or Lisp) idioms by making some functions variadic (`+`, `<=`, `&&`, etc.) and providing functions with Clojure analogues that are not available in Spark such as `remove`. For example:

```clojure
(-> dataframe
    (remove (like :Regionname "%Metropolitan%"))
    (filter (&& (< 2 :Rooms 5)
                (< 5e5 :Price 6e5)
                (< :YearBuilt 2010)))
    (select :Regionname :Rooms :Price :YearBuilt)
    show)
```

Note that functions such as `remove` and `filter` accept the Spark Dataset in the first argument. This unfortunate departure from Clojure's idioms is necessary to emulate Scala's method chaining with the threading macro `->`.

