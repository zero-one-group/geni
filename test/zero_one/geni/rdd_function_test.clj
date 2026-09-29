(ns zero-one.geni.rdd-function-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.rdd.function :as function])
  (:import
   (java.util HashSet)))

(deftest serialisability-test
  (testing "On access-field"
    (let [actual (for [field (-> HashSet .getDeclaredFields seq)]
                   (function/access-field field {}))]
      (is (and (integer? (first actual))
               (nil? (second actual))
               (not (nil? (nth actual 2)))))))
  (testing "On namespace-references"
    (is (set/subset? #{'clojure.string 'zero-one.geni.rdd.function} (function/namespace-references function/namespace-references)))
    (is (= #{} (function/namespace-references clojure.lang.Keyword))))
  (testing "On walk-object-vars"
    (doall
     (for [obj [nil true "abc" 123 :def 'ghi (ref {})]]
       (let [refs (HashSet.)
             visited (HashSet.)]
         (function/walk-object-vars refs visited obj)
         (is (empty? (into #{} refs)))
         (is (empty? (into #{} visited))))))
    (let [refs (HashSet.)
          visited (HashSet.)]
      (function/walk-object-vars refs visited {:abc function/walk-object-vars})
      (is (not (empty? (into #{} visited))))
      (is (set/subset? #{'clojure.core} (into #{} refs))))))
