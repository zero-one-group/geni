# Working with SQL Maps

Spark makes available a number of [functions](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html) that operate on SQL maps. Geni makes all these functions available in the core namespace. On top of that, Geni also adds a number of synonyms from Clojure core's map functions.

## Creating Maps

The easiest way to create a map from a flat tabular data is to use `map-from-array`, which is analogous to Clojure's `zipmap`.

```clojure
(-> melbourne-df
    (g/limit 20)
    (g/with-column :location (g/struct
                               {:address :Address
                                :suburbs :Suburb
                                :region  :Regionname
                                :council :CouncilArea}))
    (g/group-by :SellerG)
    (g/agg {:keys   (g/collect-list :Address)
            :values (g/collect-list :location)})
    (g/select {:seller :SellerG
               :map    (g/map-from-arrays :keys :values)})
    g/collect)
; =>
; [{:map {"25 Bloomburg St" {:address "25 Bloomburg St"
;                            :council "Yarra"
;                            :region "Northern Metropolitan"
;                            :suburbs "Abbotsford"}
;         "40 Federation La" {:address "40 Federation La"
;                             :council "Yarra"
;                             :region "Northern Metropolitan"
;                             :suburbs "Abbotsford"}
;         "5 Charles St" {:address "5 Charles St"
;                         :council "Yarra"
;                         :region "Northern Metropolitan"
;                         :suburbs "Abbotsford"}
;         "85 Turner St" {:address "85 Turner St"
;                         :council "Yarra"
;                         :region "Northern Metropolitan"
;                         :suburbs "Abbotsford"}}
;   :seller "Biggin"}
;  {:map {"55a Park St" {:address "55a Park St"
;                        :council "Yarra"
;                        :region "Northern Metropolitan"
;                        :suburbs "Abbotsford"}}
;   :seller "Nelson"}]
```

**TBC**
