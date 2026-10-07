(ns zero-one.geni.graph-missing-test
  "zero-one.geni.graph without GraphFrames on the classpath, as the main test
  runs have it, or over Spark Connect: an error that says what's needed. The
  graph tests themselves are in test-graph/: clojure -T:build graph-tests."
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.graph :as graph]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang ExceptionInfo)))

(deftest graphframes-needed-test
  (let [edges (g/select (g/range 3) {:src :id :dst (g/+ :id 1)})]
    (cond
      (spark/connect-only?)
      (do (is (thrown-with-msg? ExceptionInfo #"needs classic Spark\. Over Spark Connect, GraphFrames needs its own plugin"
                                (graph/graph edges)))
          (is (false? (graph/graph? edges))))

      (not (class-named "org.graphframes.GraphFrame"))
      (do (is (thrown-with-msg? ExceptionInfo #"needs GraphFrames\. Add io\.graphframes/graphframes-spark3_2\.12"
                                (graph/graph edges)))
          (is (thrown-with-msg? ExceptionInfo #"needs GraphFrames" (graph/degrees edges)))
          (is (false? (graph/graph? edges))))

      :else
      (is (graph/graph? (graph/graph edges))))))
