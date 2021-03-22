# Working with SQL Maps

Spark makes available a number of [functions](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html) that operate on SQL maps. Geni makes all these functions available in the core namespace. On top of that, Geni also adds a number of synonyms from Clojure core's map functions.

The examples assume the following required namespaces:

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])
(require '[zero-one.geni.test-resources :refer [melbourne-df]])
```

## Creating Map Columns

We can create map types using `map`, `map-from-entries` and `map-from-arrays` as follows:

Note that `melbourne-df` is a function so we need to add `()` to evaluate it.

```clojure
(def dataframe
  (-> (melbourne-df)
      (g/limit 2)
      (g/select
        {:location (g/map (g/lit "suburb") :Suburb
                          (g/lit "region") :Regionname
                          (g/lit "council") :CouncilArea
                          (g/lit "address") :Address)
         :market   (g/map-from-entries
                     (g/array (g/struct (g/lit "size") (g/double :Price))
                              (g/struct (g/lit "price") (g/double :Price))))
         :coord    (g/map-from-arrays
                     (g/array (g/lit "lat") (g/lit "long"))
                     (g/array :Lattitude :Longtitude))})))

(g/collect dataframe)
; =>
({:location
  {"suburb" "Abbotsford",
   "region" "Northern Metropolitan",
   "council" "Yarra",
   "address" "85 Turner St"},
  :market {"size" 1480000.0, "price" 1480000.0},
  :coord {"lat" -37.7996, "long" 144.9984}}
 {:location
  {"suburb" "Abbotsford",
   "region" "Northern Metropolitan",
   "council" "Yarra",
   "address" "25 Bloomburg St"},
  :market {"size" 1035000.0, "price" 1035000.0},
  :coord {"lat" -37.8079, "long" 144.9934}}]
```

## SQL Map Functions and Synonyms

Note that `map-from-arrays` above is actually the same as Clojure's `zipmap`, and it would be more natural to use the latter in a Clojure codebase. For that reason, Geni includes the following functions:

| Geni          | Original Spark    |
| ---           | ---               |
| `assoc`       | -                 |
| `dissoc`      | -                 |
| `keys`        | `map-keys`        |
| `merge-with`  | `map-zip-with`    |
| `merge`       | `map-concat`      |
| `rename-keys` | -                 |
| `select-keys` | -                 |
| `update`      | -                 |
| `vals`        | `map-values`      |
| `zipmap`      | `map-from-arrays` |
