(ns geni.utils-test
  (:require
    [clojure.string]
    [geni.core :as g]
    [midje.sweet :refer [facts fact =>]]))

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
