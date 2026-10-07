# Graphs with GraphFrames

[GraphFrames](https://graphframes.io) runs graph queries and algorithms on Spark DataFrames: a graph is a DataFrame of vertices and a DataFrame of edges. `zero-one.geni.graph` builds graphs from Geni's DataFrames and gives the results as DataFrames, which the rest of Geni works on as usual.

GraphFrames isn't one of Geni's dependencies. Its artifact is for your Spark and Scala versions: `graphframes-spark3_2.12` for Spark 3.5 on Scala 2.12, `graphframes-spark3_2.13` for Spark 3.5 on Scala 2.13, and `graphframes-spark4_2.13` for Spark 4. Geni's tests use 0.12.3:

```edn
{:deps {io.graphframes/graphframes-spark4_2.13 {:mvn/version "0.12.3"}}}
```

Without it, the functions in `zero-one.geni.graph` throw an error that says what to add. GraphFrames needs classic Spark: over Spark Connect, it needs a plugin on the server, which Geni doesn't support yet.

GraphFrames' algorithms run many Spark jobs, each of which shuffles its data into `spark.sql.shuffle.partitions` partitions, 200 by default. For a small graph on a laptop, a few partitions make them much quicker, as the examples here set:

<!-- #:test-doc-blocks{:meta :graphframes :apply :all-next} -->
```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.graph :as graph])

(g/conf-set! "spark.sql.shuffle.partitions" 4)
```

## A graph

A graph takes a DataFrame of vertices, with an `id` column, and a DataFrame of edges, with `src` and `dst` columns of vertex ids. Their other columns are attributes:

```clojure
(def people
  (graph/graph
   (g/table->dataset [["a" "Alice" 34] ["b" "Bob" 36] ["c" "Charlie" 30] ["d" "David" 29] ["e" "Esther" 32]]
                     [:id :name :age])
   (g/table->dataset [["a" "b" "friend"] ["b" "c" "follow"] ["c" "b" "follow"] ["a" "c" "friend"] ["d" "a" "friend"]]
                     [:src :dst :relationship])))

(-> people graph/in-degrees (g/order-by :id) g/collect)
;; => ({:id "a", :inDegree 1} {:id "b", :inDegree 2} {:id "c", :inDegree 2})
```

`graph/vertices` and `graph/edges` give them back, `graph/triplets` gives each edge with its two vertices, and `graph/degrees` and `graph/out-degrees` count the edges. A vertex without edges, as Esther has, isn't in the degrees. Given only edges, `graph/graph` takes the vertices from their ids.

`graph/filter-vertices` keeps the vertices that a condition holds for, a column or a SQL expression, and the edges between them, `graph/filter-edges` keeps edges, and `graph/drop-isolated-vertices` drops the vertices without edges:

```clojure
(-> people
    (graph/filter-vertices (g/> :age 30))
    graph/edges
    g/collect)
;; => ({:src "a", :dst "b", :relationship "friend"})
```

## Motifs

`graph/find` finds a pattern of edges, with a struct column for each vertex and edge that the pattern names. This one finds the pairs who follow each other:

```clojure
(-> people
    (graph/find "(x)-[e]->(y); (y)-[e2]->(x)")
    (g/select {:x :x.name :y :y.name})
    (g/order-by :x)
    g/collect)
;; => ({:x "Bob", :y "Charlie"} {:x "Charlie", :y "Bob"})
```

A pattern can say that an edge mustn't be there, as in `"(x)-[]->(y); !(y)-[]->(x)"`, and `()` and `[]` match without a column.

## Paths

`graph/bfs` gives the shortest paths, by breadth-first search, from the vertices that one condition holds for to the vertices that another holds for:

```clojure
(-> people
    (graph/bfs "name = 'David'" "name = 'Charlie'")
    (g/select {:from :from.name :via :v1.name :to :to.name})
    g/collect)
;; => ({:from "David", :via "Alice", :to "Charlie"})
```

It takes `:max-path-length` and an `:edge-filter` in its options. `graph/shortest-paths` gives each vertex's distances to some landmarks, as a map from the ones it reaches:

```clojure
(-> people
    (graph/shortest-paths ["a" "c"])
    (g/select :id :distances)
    (g/order-by :id)
    g/collect)
;; => ({:id "a", :distances {"a" 0, "c" 1}}
;;     {:id "b", :distances {"c" 1}}
;;     {:id "c", :distances {"c" 0}}
;;     {:id "d", :distances {"a" 1, "c" 2}}
;;     {:id "e", :distances {}})
```

## Algorithms

Each algorithm takes an option map, with GraphFrames' setters in kebab case, such as `:max-iter`. An option that it doesn't have throws an error that lists the ones it has. PageRank gives a graph, whose vertices have a `pagerank` column:

```clojure
(-> people
    (graph/page-rank {:max-iter 10})
    graph/vertices
    (g/select :name (g/round :pagerank 3))
    (g/order-by (g/desc :pagerank) :name)
    (g/limit 2)
    g/collect-vals)
;; => (["Bob" 2.152] ["Charlie" 2.152])
```

`:tol` runs until no rank changes by more than that, and `:source-id` personalises the ranks to one vertex.

`graph/connected-components` gives each vertex's component, ignoring the edges' directions. GraphFrames' default algorithm checkpoints its work, so it needs a checkpoint directory, from `g/create-spark-session`'s `:checkpoint-dir` or the `spark.checkpoint.dir` config, or else `:use-local-checkpoints`, which checkpoints in the executors' storage instead. With `:use-labels-as-components`, a component is named after one of its vertices:

```clojure
(-> people
    (graph/connected-components {:use-local-checkpoints true :use-labels-as-components true})
    (g/select :name :component)
    (g/order-by :name)
    g/collect-vals)
;; => (["Alice" "a"] ["Bob" "a"] ["Charlie" "a"] ["David" "a"] ["Esther" "e"])
```

The others work the same way:

- `graph/strongly-connected-components` follows the edges' directions, and needs `:max-iter`.
- `graph/label-propagation` finds communities, and needs `:max-iter`.
- `graph/triangle-count` counts each vertex's triangles.

```clojure
(-> people graph/triangle-count (g/select :name :count) (g/order-by :name) g/collect-vals)
;; => (["Alice" 1] ["Bob" 1] ["Charlie" 1] ["David" 0] ["Esther" 0])
```

## Messages

`graph/aggregate-messages` sends a message along each edge, a column of its source vertex, `graph/src`, its destination, `graph/dst`, or the edge itself, `graph/edge`, and aggregates each vertex's messages, `graph/msg`. Here each person gets the sum of their followers' ages:

```clojure
(-> people
    (graph/aggregate-messages {:send-to-dst (graph/src :age)
                               :agg         (g/as (g/sum (graph/msg)) :followers-ages)})
    (g/order-by :id)
    g/collect)
;; => ({:id "a", :followers-ages 29} {:id "b", :followers-ages 64} {:id "c", :followers-ages 70})
```

`graph/pregel` repeats that, as Pregel's supersteps. Each vertex column has an initial value and an update, which can use `graph/pregel-msg`, the aggregate of the vertex's messages, null when it has none. This one counts each vertex's edges in, three times over:

```clojure
(-> people
    (graph/pregel {:max-iter            3
                   :checkpoint-interval 0
                   :vertex-columns      {:in [(g/lit 0) (g/+ :in (g/coalesce (graph/pregel-msg) (g/lit 0)))]}
                   :send-to-dst         (g/lit 1)
                   :agg-msgs            (g/sum (graph/pregel-msg))})
    (g/select :id :in)
    (g/order-by :id)
    g/collect-vals)
;; => (["a" 3] ["b" 6] ["c" 6] ["d" 0] ["e" 0])
```

Pregel checkpoints as connected components does, so it takes the same options, and `:checkpoint-interval 0` turns that off, which suits a few supersteps.
