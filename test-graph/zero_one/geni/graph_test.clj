(ns ^:classic ^:graphframes zero-one.geni.graph-test
  "zero-one.geni.graph, with GraphFrames on the classpath:
  clojure -T:build graph-tests."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [zero-one.geni.core :as g]
   [zero-one.geni.graph :as graph]
   [zero-one.geni.test-resources :refer [checkpoint-dir! spark with-fresh-session]])
  (:import
   (clojure.lang ExceptionInfo)))

(defn- few-partitions!
  "Four shuffle partitions rather than 200, which keep the iterative
  algorithms quick on these tiny graphs."
  []
  (g/conf-set! "spark.sql.shuffle.partitions" 4))

(use-fixtures :each (fn [f]
                      (few-partitions!)
                      (try (f) (finally (g/conf-unset! "spark.sql.shuffle.partitions")))))

(defn- people
  "Five people, four of whom follow or befriend one another, and Esther, who
  has no edges."
  []
  (graph/graph
   (g/table->dataset @spark
                     [["a" "Alice" 34] ["b" "Bob" 36] ["c" "Charlie" 30] ["d" "David" 29] ["e" "Esther" 32]]
                     [:id :name :age])
   (g/table->dataset @spark
                     [["a" "b" "friend"] ["b" "c" "follow"] ["c" "b" "follow"] ["a" "c" "friend"] ["d" "a" "friend"]]
                     [:src :dst :relationship])))

(defn- by-id
  "A DataFrame's rows as a map of `id` to the value of `k`."
  [df k]
  (into {} (map (juxt :id k)) (g/collect df)))

(deftest graph-test
  (let [people (people)]
    (is (graph/graph? people))
    (is (not (graph/graph? (graph/vertices people))))
    (is (= [5 5 5] (map g/count [(graph/vertices people) (graph/edges people) (graph/triplets people)])))
    (is (= ["src" "edge" "dst"] (g/column-names (graph/triplets people))))
    (testing "from the edges alone, with the vertices that they name"
      (is (= #{"a" "b" "c" "d"} (set (g/collect-col (graph/vertices (graph/graph (graph/edges people))) :id))))))
  (testing "an error for something that isn't a graph"
    (is (thrown-with-msg? ExceptionInfo #"vertices takes a graph, as graph/graph makes one"
                          (graph/vertices (g/range 3))))))

(deftest degrees-test
  (let [people (people)]
    (is (= {"a" 3 "b" 3 "c" 3 "d" 1} (by-id (graph/degrees people) :degree)))
    (is (= {"a" 1 "b" 2 "c" 2} (by-id (graph/in-degrees people) :inDegree)))
    (is (= {"a" 2 "b" 1 "c" 1 "d" 1} (by-id (graph/out-degrees people) :outDegree)))))

(deftest filters-test
  (let [people (people)
        older  (graph/filter-vertices people (g/> :age 30))]
    (is (= #{"a" "b" "e"} (set (g/collect-col (graph/vertices older) :id))))
    (is (= [["a" "b"]] (g/collect-vals (g/select (graph/edges older) :src :dst))))
    (is (= 3 (g/count (graph/edges (graph/filter-edges people "relationship = 'friend'")))))
    (is (= 4 (g/count (graph/vertices (graph/drop-isolated-vertices people)))))))

(deftest find-test
  (let [people (people)]
    (testing "a motif"
      (is (= #{["b" "c"] ["c" "b"]}
             (set (g/collect-vals (g/select (graph/find people "(x)-[e]->(y); (y)-[e2]->(x)")
                                            {:x :x.id :y :y.id}))))))
    (testing "an edge that mustn't be there"
      (is (= #{["a" "b"] ["a" "c"] ["d" "a"]}
             (set (g/collect-vals (g/select (graph/find people "(x)-[]->(y); !(y)-[]->(x)")
                                            {:x :x.id :y :y.id}))))))))

(deftest bfs-test
  (let [people (people)]
    (is (= [["a" "c"]]
           (g/collect-vals (g/select (graph/bfs people "name = 'Alice'" (g/< :age 32)) {:from :from.id :to :to.id}))))
    (testing "with an edge filter, and a maximum length"
      (is (= [["d" "a" "b"]]
             (g/collect-vals (g/select (graph/bfs people (g/= :id (g/lit "d")) (g/= :id (g/lit "b"))
                                                  {:edge-filter "relationship = 'friend'"})
                                       {:from :from.id :via :v1.id :to :to.id}))))
      (is (zero? (g/count (graph/bfs people (g/= :id (g/lit "d")) (g/= :id (g/lit "b")) {:max-path-length 1})))))))

(deftest page-rank-test
  (let [people (people)
        ranked (graph/page-rank people {:max-iter 10})
        ranks  (by-id (graph/vertices ranked) :pagerank)]
    (is (graph/graph? ranked))
    (is (= 5 (count ranks)))
    (is (= #{"b" "c"} (set (take 2 (map key (sort-by (comp - val) ranks))))))
    (is (some #{"weight"} (g/column-names (graph/edges ranked))))
    (testing "until the ranks settle, and personalised"
      (is (= 5 (g/count (graph/vertices (graph/page-rank people {:tol 0.01})))))
      (let [from-d (by-id (graph/vertices (graph/page-rank people {:max-iter 10 :source-id "d"})) :pagerank)]
        (is (zero? (from-d "e")))
        (is (pos? (from-d "a")))))))

(deftest connected-components-test
  (let [g (people)]
    (is (= {"a" "a" "b" "a" "c" "a" "d" "a" "e" "e"}
           (by-id (graph/connected-components g {:use-local-checkpoints true
                                                 :use-labels-as-components true})
                  :component)))
    (is (= 2 (count (set (vals (by-id (graph/connected-components g {:algorithm "graphx"})
                                      :component))))))
    (testing "an option that it doesn't have"
      (is (thrown-with-msg? ExceptionInfo #"takes the options .*:max-iter.* Got: :max-iters"
                            (graph/connected-components g {:max-iters 3})))))
  (testing "an error that says how to give it a checkpoint directory, and the checkpoints there"
    (with-fresh-session
      (few-partitions!)
      (let [g (people)]
        (is (thrown-with-msg? ExceptionInfo #"connected-components checkpoints, so it needs a checkpoint directory"
                              (graph/connected-components g)))
        (checkpoint-dir!)
        (is (= 2 (count (set (vals (by-id (graph/connected-components g) :component))))))))))

(deftest strongly-connected-components-test
  (let [components (by-id (graph/strongly-connected-components (people) {:max-iter 10}) :component)]
    (is (= (components "b") (components "c")))
    (is (= 4 (count (set (vals components))))))
  (is (thrown-with-msg? ExceptionInfo #"strongly-connected-components needs :max-iter"
                        (graph/strongly-connected-components (people) {}))))

(deftest label-propagation-test
  (let [labels (by-id (graph/label-propagation (people) {:max-iter 5 :algorithm "graphx"}) :label)]
    (is (= 5 (count labels)))
    (is (not-any? #{(labels "e")} (vals (dissoc labels "e"))) "Esther, alone, has her own"))
  (is (= 5 (g/count (graph/label-propagation (people) {:max-iter 2 :use-local-checkpoints true})))))

(deftest shortest-paths-test
  (let [people (people)]
    (is (= {"a" {"a" 0 "c" 1} "b" {"c" 1} "c" {"c" 0} "d" {"a" 1 "c" 2} "e" {}}
           (by-id (graph/shortest-paths people ["a" "c"]) :distances)))
    (is (= {"a" 1 "c" 1} (get (by-id (graph/shortest-paths people ["a" "c"] {:is-directed false}) :distances)
                              "b")))))

(deftest triangle-count-test
  (let [counted (graph/triangle-count (people))]
    (is (= ["id" "name" "age" "count"] (g/column-names counted)))
    (is (= {"a" 1 "b" 1 "c" 1 "d" 0 "e" 0} (by-id counted :count)))))

(deftest aggregate-messages-test
  (let [people (people)]
    (is (= {"a" 29 "b" 64 "c" 70}
           (by-id (graph/aggregate-messages people {:send-to-dst (graph/src :age)
                                                    :agg         (g/as (g/sum (graph/msg)) :ages)})
                  :ages)))
    (is (= {"a" 95 "b" 94 "c" 106 "d" 34}
           (by-id (graph/aggregate-messages people {:send-to-dst (graph/src :age)
                                                    :send-to-src (graph/dst :age)
                                                    :agg         (g/as (g/sum (graph/msg)) :ages)})
                  :ages)))
    (testing "the edge's attributes"
      (is (= {"a" 1 "b" 1 "c" 1}
             (by-id (graph/aggregate-messages people {:send-to-dst (g/when (g/= (graph/edge :relationship) (g/lit "friend")) 1 0)
                                                      :agg         (g/as (g/sum (graph/msg)) :friends)})
                    :friends))))
    (is (thrown-with-msg? ExceptionInfo #"aggregate-messages needs :agg"
                          (graph/aggregate-messages people {:send-to-dst (graph/src :age)})))))

(deftest pregel-test
  (let [people   (people)
        counting {:max-iter            1
                  :checkpoint-interval 0
                  :vertex-columns      {:in [(g/lit 0) (g/+ :in (g/coalesce (graph/pregel-msg) (g/lit 0)))]}
                  :send-to-dst         (g/lit 1)
                  :agg-msgs            (g/sum (graph/pregel-msg))}]
    (testing "one superstep counts each vertex's edges in"
      (is (= {"a" 1 "b" 2 "c" 2 "d" 0 "e" 0} (by-id (graph/pregel people counting) :in))))
    (testing "and each superstep adds them again"
      (is (= {"a" 3 "b" 6 "c" 6 "d" 0 "e" 0} (by-id (graph/pregel people (assoc counting :max-iter 3)) :in))))
    (is (thrown-with-msg? ExceptionInfo #"pregel needs :agg-msgs"
                          (graph/pregel people (dissoc counting :agg-msgs))))))
