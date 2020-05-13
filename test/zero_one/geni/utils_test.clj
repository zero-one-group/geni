(ns zero-one.geni.utils-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.interop :as interop])
  (:import
    (scala.collection Seq)))

(facts "On ensure-coll"
  (fact "should not change collections"
    (g/ensure-coll []) => []
    (g/ensure-coll #{"a"}) => #{"a"}
    (g/ensure-coll {:a 1}) => {:a 1}
    (g/ensure-coll (list 1 2)) => (list 1 2)
    (g/ensure-coll nil) => nil)
  (fact "should wrap non-collections in vector"
    (g/ensure-coll 1) => [1]
    (g/ensure-coll "a") => ["a"]))

(fact "On ->java"
  (let [converted (interop/->java Seq [0 1 2])]
    converted => #(instance? Seq %)))

(fact "On ->clojure"
  (let [data      [(interop/->scala-seq [1 2 3])]
        converted (interop/->clojure data)]
    converted => (map interop/->clojure data)))
