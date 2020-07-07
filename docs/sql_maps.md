# Working with SQL Maps

Spark makes available a number of [functions](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html) that operate on SQL maps. Geni makes all these functions available in the core namespace. On top of that, Geni also adds a number of synonyms from Clojure core's map functions.

## Creating Map Columns

We can create map types using `map`, `map-from-entries` and `map-from-arrays` as follows:

```clojure
(def dataframe
  (-> melbourne-df
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

Note that `map-from-arrays` above is actually the same as Clojure's `zipmap`, and it would be more natural to use the latter in a Clojure codebase. For that reason, Geni includes the following synonyms:

| Spark Function    | Synonym      |
| ---               | ---          |
| `map-concat`      | `merge`      |
| `map-from-arrays` | `zipmap`     |
| `map-keys`        | `keys`       |
| `map-values`      | `vals`       |
| `map-zip-with`    | `merge-with` |

Moreover Geni makes a number of Clojure's map functions available that are not present in the original Spark functions:

| Geni Function |
| ---           |
| `assoc`       |
| `dissoc`      |
| `rename-keys` |
| `select-keys` |
| `update`      |
