(ns zero-one.geni.graph
  "Graphs with GraphFrames, an optional dependency: a graph from DataFrames of
  vertices and edges, its degrees, motif finding, and graph algorithms, such as
  PageRank, connected components and Pregel. Each algorithm takes an option
  map, with GraphFrames' setters in kebab case, such as `:max-iter`, and gives
  a DataFrame, but for PageRank, which gives a graph. See the graphs guide.

  GraphFrames is looked up when a function needs it, so without it on the
  classpath, a function throws an error that says what to add. It works with
  classic Spark only: over Spark Connect, GraphFrames needs its own plugin on
  the server."
  (:refer-clojure :exclude [find])
  (:require
   [clojure.string :as string]
   [zero-one.geni.core.column :as column]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang Reflector)
   (java.io IOException)
   (java.util ArrayList)
   (org.apache.spark.sql Column Dataset)))

;;;; GraphFrames, looked up when needed

(defn- graphframe-class
  "GraphFrames' `GraphFrame` class, or an error that says what's needed."
  ^Class []
  (cond
    (spark/connect-only?)
    (throw (ex-info (str "zero-one.geni.graph needs classic Spark. Over Spark Connect, GraphFrames "
                         "needs its own plugin on the server, which Geni doesn't support yet.")
                    {}))

    :else
    (or (class-named "org.graphframes.GraphFrame")
        (throw (ex-info (str "zero-one.geni.graph needs GraphFrames. Add "
                             "io.graphframes/graphframes-spark3_2.12, graphframes-spark3_2.13 or "
                             "graphframes-spark4_2.13, for your Spark and Scala, version 0.12.3 or "
                             "later, to your dependencies.")
                        {})))))

(defn- checked
  "The graph, after checking that it is one, for `fn-name`."
  [fn-name g]
  (if (instance? (graphframe-class) g)
    g
    (throw (ex-info (str fn-name " takes a graph, as graph/graph makes one. Got: "
                         (if (nil? g) "nil" (.getName (class g))))
                    {:value g}))))

(defn- expr
  "A filter or a condition for GraphFrames, which takes a column or a SQL
  expression: a keyword names a boolean column, and a string stays SQL."
  [x]
  (if (keyword? x) (column/col x) x))

(defn- configure
  "Sets each option on a GraphFrames algorithm builder through `setters`, a
  map of option keys to their setter's name and a function that converts the
  value, but for the options in `handled`, which the caller sets. Any other
  option throws, naming the ones there are."
  ([builder fn-name setters options] (configure builder fn-name setters options #{}))
  ([builder fn-name setters options handled]
   (doseq [[k v] options
           :when (not (handled k))]
     (if-let [[method convert] (get setters k)]
       (Reflector/invokeInstanceMethod builder method (object-array [(convert v)]))
       (let [known (sort (concat (keys setters) handled))]
         (throw (ex-info (str fn-name " takes the options " (string/join ", " known) ". Got: " k)
                         {:option k :options known})))))
   builder))

(defn- required!
  "Throws unless `options` has each of `ks`."
  [fn-name options ks]
  (doseq [k ks]
    (when (nil? (get options k))
      (throw (ex-info (str fn-name " needs " k " in its options.") {:option k})))))

(defn- run-checkpointed
  "Runs an algorithm that checkpoints, with an error that says how to give it
  a checkpoint directory when it has none."
  [builder fn-name]
  (try
    (Reflector/invokeNoArgInstanceMember builder "run" false)
    (catch IOException e
      (if (some-> (ex-message e) (string/includes? "Checkpoint directory is not set"))
        (throw (ex-info (str fn-name " checkpoints, so it needs a checkpoint directory: give "
                             "g/create-spark-session a :checkpoint-dir, or set the "
                             "spark.checkpoint.dir config, or pass :use-local-checkpoints true "
                             "to checkpoint in the executors' storage, or :checkpoint-interval 0 "
                             "not to checkpoint.")
                        {}
                        e))
        (throw e)))))

(def ^:private checkpoints
  {:checkpoint-interval        ["setCheckpointInterval" int]
   :use-local-checkpoints      ["setUseLocalCheckpoints" boolean]
   :intermediate-storage-level ["setIntermediateStorageLevel" identity]})

(def ^:private algorithm
  {:algorithm ["setAlgorithm" name]})

;;;; Graphs

(defn graph
  "A graph from a DataFrame of vertices, with an `id` column, and a DataFrame
  of edges, with `src` and `dst` columns of vertex ids. Other columns are the
  vertices' and edges' attributes. Given only the edges, the vertices are the
  ids that they name.

  ```clojure
  (graph/graph (g/table->dataset [[\"a\" \"Alice\"] [\"b\" \"Bob\"]] [:id :name])
               (g/table->dataset [[\"a\" \"b\" \"follows\"]] [:src :dst :relationship]))
  ```"
  ([edges]
   (Reflector/invokeStaticMethod (graphframe-class) "fromEdges" (object-array [edges])))
  ([vertices edges]
   (Reflector/invokeStaticMethod (graphframe-class) "apply" (object-array [vertices edges]))))

(defn graph?
  "Whether `x` is a graph, as `graph` makes one."
  [x]
  (boolean (some-> (class-named "org.graphframes.GraphFrame") (instance? x))))

(defn vertices
  "The graph's vertices, as a DataFrame."
  [g]
  (.vertices (checked "vertices" g)))

(defn edges
  "The graph's edges, as a DataFrame."
  [g]
  (.edges (checked "edges" g)))

(defn triplets
  "The graph's edges with both their vertices: a DataFrame of `src`, `edge`
  and `dst` structs."
  [g]
  (.triplets (checked "triplets" g)))

(defn degrees
  "Each vertex's number of edges, in and out, as `id` and `degree`. A vertex
  without edges isn't there."
  [g]
  (.degrees (checked "degrees" g)))

(defn in-degrees
  "Each vertex's number of edges in, as `id` and `inDegree`. A vertex without
  edges in isn't there."
  [g]
  (.inDegrees (checked "in-degrees" g)))

(defn out-degrees
  "Each vertex's number of edges out, as `id` and `outDegree`. A vertex
  without edges out isn't there."
  [g]
  (.outDegrees (checked "out-degrees" g)))

(defn filter-vertices
  "The graph with only the vertices that `condition` holds for, a column or a
  SQL expression, and only the edges between them."
  [g condition]
  (.filterVertices (checked "filter-vertices" g) (expr condition)))

(defn filter-edges
  "The graph with only the edges that `condition` holds for, a column or a
  SQL expression, and all its vertices."
  [g condition]
  (.filterEdges (checked "filter-edges" g) (expr condition)))

(defn drop-isolated-vertices
  "The graph without the vertices that have no edges."
  [g]
  (.dropIsolatedVertices (checked "drop-isolated-vertices" g)))

;;;; Searching

(defn find
  "The occurrences of a motif in the graph, a pattern of edges such as
  `\"(a)-[e]->(b); (b)-[e2]->(a)\"`, as a DataFrame with a struct column for
  each named vertex and edge. `!(a)-[]->(b)` excludes an edge, and an
  anonymous `()` or `[]` matches without a column.

  ```clojure
  (graph/find g \"(a)-[e]->(b); (b)-[e2]->(a)\")
  ```"
  [g pattern]
  (.find (checked "find" g) ^String pattern))

(defn bfs
  "The shortest paths, by breadth-first search, from the vertices that `from`
  holds for to those that `to` holds for, each a column or a SQL expression,
  as a DataFrame with a column for each vertex and edge on the path: `from`,
  `e0`, `v1`, `e1` and so on to `to`. The options are `:max-path-length`, 10
  by default, and `:edge-filter`, a column or a SQL expression over the
  edges' columns that the path's edges must meet.

  ```clojure
  (graph/bfs g \"name = 'Alice'\" \"age < 32\" {:max-path-length 3})
  ```"
  ([g from to] (bfs g from to {}))
  ([g from to options]
   (-> (.bfs (checked "bfs" g))
       (doto (.fromExpr (expr from)) (.toExpr (expr to)))
       (configure "bfs"
                  {:max-path-length ["maxPathLength" int]
                   :edge-filter     ["edgeFilter" expr]}
                  options)
       .run)))

;;;; Algorithms

(defn page-rank
  "PageRank, as a graph whose vertices have a `pagerank` column and whose
  edges have a `weight` column. It needs `:max-iter`, for a fixed number of
  iterations, or `:tol`, to run until no rank changes by more than that. The
  other options are `:reset-probability`, 0.15 by default, and `:source-id`,
  a vertex id for a personalised PageRank.

  ```clojure
  (-> g (graph/page-rank {:max-iter 10}) graph/vertices)
  ```"
  [g options]
  (-> (.pageRank (checked "page-rank" g))
      (configure "page-rank"
                 {:max-iter          ["maxIter" int]
                  :tol               ["tol" double]
                  :reset-probability ["resetProbability" double]
                  :source-id         ["sourceId" identity]}
                 options)
      .run))

(defn connected-components
  "Each vertex's connected component, ignoring the edges' directions, as a
  `component` column, which is a number unless `:use-labels-as-components` is
  true, when it's a vertex id.

  It checkpoints, so it needs a checkpoint directory, from
  `g/create-spark-session`'s `:checkpoint-dir` or the `spark.checkpoint.dir`
  config, unless `:use-local-checkpoints` is true, to checkpoint in the
  executors' storage, or `:checkpoint-interval` is 0. The other options are
  `:algorithm`, such as \"graphx\", `:max-iter`, `:broadcast-threshold` and
  `:intermediate-storage-level`."
  ([g] (connected-components g {}))
  ([g options]
   (-> (.connectedComponents (checked "connected-components" g))
       (configure "connected-components"
                  (merge checkpoints
                         algorithm
                         {:max-iter                 ["maxIter" int]
                          :broadcast-threshold      ["setBroadcastThreshold" int]
                          :use-labels-as-components ["setUseLabelsAsComponents" boolean]})
                  options)
       (run-checkpointed "connected-components"))))

(defn strongly-connected-components
  "Each vertex's strongly connected component, following the edges'
  directions, as a `component` column. It needs `:max-iter`."
  [g options]
  (required! "strongly-connected-components" options [:max-iter])
  (-> (.stronglyConnectedComponents (checked "strongly-connected-components" g))
      (configure "strongly-connected-components" {:max-iter ["maxIter" int]} options)
      .run))

(defn label-propagation
  "Communities by label propagation, as each vertex's `label`. It needs
  `:max-iter`, and takes `:algorithm`, \"graphframes\" or \"graphx\", and the
  checkpoint options of `connected-components`."
  [g options]
  (required! "label-propagation" options [:max-iter])
  (-> (.labelPropagation (checked "label-propagation" g))
      (configure "label-propagation"
                 (merge checkpoints algorithm {:max-iter ["maxIter" int]})
                 options)
      (run-checkpointed "label-propagation")))

(defn shortest-paths
  "Each vertex's number of edges to each of `landmarks`, a collection of
  vertex ids, as a `distances` map from the landmarks that it reaches. The
  options are `:is-directed`, true by default, `:algorithm`, \"graphx\" or
  \"graphframes\", and the checkpoint options of `connected-components`."
  ([g landmarks] (shortest-paths g landmarks {}))
  ([g landmarks options]
   (-> (.shortestPaths (checked "shortest-paths" g))
       (doto (.landmarks (ArrayList. ^java.util.Collection (vec landmarks))))
       (configure "shortest-paths"
                  (merge checkpoints algorithm {:is-directed ["setIsDirected" boolean]})
                  options)
       (run-checkpointed "shortest-paths"))))

(defn triangle-count
  "Each vertex's number of triangles, ignoring the edges' directions, as a
  `count` column with the vertices' own. The options are `:algorithm`,
  `:lg-nom-entries`, for the approximate one's sketches, and
  `:intermediate-storage-level`."
  ([g] (triangle-count g {}))
  ([g options]
   (let [g      (checked "triangle-count" g)
         result (-> (.triangleCount g)
                    (configure "triangle-count"
                               (merge algorithm
                                      {:lg-nom-entries             ["setLgNomEntries" int]
                                       :intermediate-storage-level ["setIntermediateStorageLevel" identity]})
                               options)
                    .run)]
     ;; GraphFrames 0.12.3 leaves its working columns in.
     (.select ^Dataset result
              ^"[Lorg.apache.spark.sql.Column;"
              (into-array Column (map column/col (concat (.vertexColumns g) ["count"])))))))

;;;; Messages

(defn src
  "In `aggregate-messages` and `pregel`, the column of an edge's source
  vertex, or of one of its attributes."
  ([] (column/col "src"))
  ([attribute] (column/col (str "src." (name attribute)))))

(defn dst
  "In `aggregate-messages` and `pregel`, the column of an edge's destination
  vertex, or of one of its attributes."
  ([] (column/col "dst"))
  ([attribute] (column/col (str "dst." (name attribute)))))

(defn edge
  "In `aggregate-messages` and `pregel`, the column of an edge, or of one of
  its attributes."
  ([] (column/col "edge"))
  ([attribute] (column/col (str "edge." (name attribute)))))

(defn msg
  "In `aggregate-messages`, the column of the messages that `:agg` aggregates."
  []
  (column/col "MSG"))

(defn pregel-msg
  "In `pregel`, the column of the messages that `:agg-msgs` aggregates."
  []
  (column/col "_pregel_msg_"))

(defn aggregate-messages
  "Sends a message along each edge, to its source, its destination or both,
  and aggregates each vertex's messages, as a DataFrame of `id` and the
  aggregate. The options are `:send-to-src` and `:send-to-dst`, the messages
  as columns of `src`, `dst` and `edge`, and `:agg`, an aggregate of `msg`.

  ```clojure
  (graph/aggregate-messages g {:send-to-dst (graph/src :age)
                               :agg         (g/as (g/sum (graph/msg)) :summed-ages)})
  ```"
  [g options]
  (required! "aggregate-messages" options [:agg])
  (-> (.aggregateMessages (checked "aggregate-messages" g))
      (configure "aggregate-messages"
                 {:send-to-src                ["sendToSrc" column/col]
                  :send-to-dst                ["sendToDst" column/col]
                  :intermediate-storage-level ["setIntermediateStorageLevel" identity]}
                 options
                 #{:agg})
      (.agg ^Column (column/col (:agg options)))))

(defn pregel
  "Runs a Pregel computation and gives the vertices with their new columns.

  `:vertex-columns` maps each new column to its initial value and its update,
  each a column, where the update can use the column itself and `pregel-msg`.
  Each superstep sends `:send-to-src` and `:send-to-dst`, columns of `src`,
  `dst` and `edge`, which can use the new columns, aggregates each vertex's
  messages with `:agg-msgs`, and updates the vertex columns. A vertex without
  messages gets a null aggregate. It stops after `:max-iter` supersteps, 10 by
  default, or with `:early-stopping`, when no messages are sent. It
  checkpoints as `connected-components` does, with the same options. The
  others are `:intermediate-storage-level`,
  `:stop-if-all-non-active-vertices`, `:initial-active-vertex-expression`,
  `:update-active-vertex-expression` and
  `:skip-messages-from-non-active-vertices`.

  ```clojure
  (graph/pregel g {:max-iter       3
                   :vertex-columns {:total [(g/lit 0) (g/+ :total (graph/pregel-msg))]}
                   :send-to-dst    (g/lit 1)
                   :agg-msgs       (g/sum (graph/pregel-msg))})
  ```"
  [g options]
  (required! "pregel" options [:vertex-columns :agg-msgs])
  (let [builder (.pregel (checked "pregel" g))]
    (doseq [[col-name [initial update]] (:vertex-columns options)]
      (.withVertexColumn builder (name col-name) (column/col initial) (column/col update)))
    (-> builder
        (configure "pregel"
                   (merge checkpoints
                          {:max-iter                               ["setMaxIter" int]
                           :early-stopping                         ["setEarlyStopping" boolean]
                           :stop-if-all-non-active-vertices        ["setStopIfAllNonActiveVertices" boolean]
                           :initial-active-vertex-expression       ["setInitialActiveVertexExpression" column/col]
                           :update-active-vertex-expression        ["setUpdateActiveVertexExpression" column/col]
                           :skip-messages-from-non-active-vertices ["setSkipMessagesFromNonActiveVertices" boolean]
                           :send-to-src                            ["sendMsgToSrc" column/col]
                           :send-to-dst                            ["sendMsgToDst" column/col]
                           :agg-msgs                               ["aggMsgs" column/col]})
                   options
                   #{:vertex-columns})
        (run-checkpointed "pregel"))))
