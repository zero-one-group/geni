(ns zero-one.geni.utils-test
  (:require
   [clojure.string]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [ensure-coll with-dynamic-import]])
  (:import
   (scala.collection Seq)))

(facts "On dynamic imports"
  (fact "succeeds with valid import forms"
    (with-dynamic-import
      [[org.apache.spark.sql functions]]
      (def adf-def 123)) => #(and (= % :succeeded) (= adf-def 123))
    (with-dynamic-import
      [org.apache.spark.sql.Column]
      (def ghi-jkl 123)) => :succeeded) ;#(and (= % :succeeded) (= ghi-jkl 123))))
  (fact "fails gracefully"
    (with-dynamic-import
      [[some.non-existent.namespace non-existent-class]]
      (def mno-pqr 123)) => #(and (= % :failed) (nil? (resolve 'mno-pqr)))
    (with-dynamic-import
      [some.non-existent.namespace.NonExistentClass]
      (def stu-vwx 123)) => #(and (= % :failed) (nil? (resolve 'stu-vwx)))
    (with-dynamic-import
      (+ 1 1)
      (def xyz 123)) => #(and (= % :failed) (nil? (resolve 'xyz)))))

(facts "On ensure-coll"
  (fact "should not change collections"
    (ensure-coll []) => []
    (ensure-coll #{"a"}) => #{"a"}
    (ensure-coll {:a 1}) => {:a 1}
    (ensure-coll (list 1 2)) => (list 1 2)
    (ensure-coll nil) => nil)
  (fact "should wrap non-collections in vector"
    (ensure-coll 1) => [1]
    (ensure-coll "a") => ["a"]))

(fact "On ->java"
  (let [converted (interop/->java Seq [0 1 2])]
    converted => #(instance? Seq %)))

(fact "On ->clojure"
  (let [data      [(interop/->scala-seq [1 2 3])]
        converted (interop/->clojure data)]
    converted => (map interop/->clojure data)))
