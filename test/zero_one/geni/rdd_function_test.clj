(ns zero-one.geni.rdd-function-test
  (:require
   [clojure.set :as set]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.rdd.function :as function])
  (:import
   (java.util HashSet)))

(facts "On serialisability"
  (fact "On access-field"
    (for [field (-> HashSet .getDeclaredFields seq)]
      (function/access-field field {})) => #(and (integer? (first %))
                                                 (nil? (second %))
                                                 (not (nil? (nth % 2)))))
  (fact "On namespace-references"
    (function/namespace-references function/namespace-references)
    => (partial set/subset? #{'clojure.string 'zero-one.geni.rdd.function})
    (function/namespace-references clojure.lang.Keyword) => #{})
  (fact "On walk-object-vars"
    (doall
     (for [obj [nil true "abc" 123 :def 'ghi (ref {})]]
       (let [refs (HashSet.)
             visited (HashSet.)]
         (function/walk-object-vars refs visited obj)
         (into #{} refs) => empty?
         (into #{} visited) => empty?)))
    (let [refs (HashSet.)
          visited (HashSet.)]
      (function/walk-object-vars refs visited {:abc function/walk-object-vars})
      (into #{} visited) => (complement empty?)
      (into #{} refs) => (partial set/subset? #{'clojure.core}))))
