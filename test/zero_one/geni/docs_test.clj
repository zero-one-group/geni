(ns ^:classic zero-one.geni.docs-test
  (:require
   [clojure.string :as string]
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.graph]
   [zero-one.geni.ml]
   [zero-one.geni.rdd]))

(defn some-docless-fn [])

(defn some-fn-with-doc
  "some dummy doc"
  [])

(defn some-fn-with-invalid-doc
  [])
(docs/add-doc!
 (var some-fn-with-invalid-doc)
 ["This doc is not a string."])

(deftest correct-docless-vars-identification-test
  (is (= [(var some-docless-fn)]
         (remove (comp :test meta) (docs/docless-vars 'zero-one.geni.docs-test))))
  (is (= {'some-fn-with-invalid-doc (var some-fn-with-invalid-doc)} (docs/invalid-doc-vars 'zero-one.geni.docs-test))))

(deftest frequently-required-namespaces-must-have-complete-test
  (is (empty? (docs/docless-vars 'zero-one.geni.core)))
  (is (empty? (docs/docless-vars 'zero-one.geni.ml)))
  (is (empty? (docs/docless-vars 'zero-one.geni.rdd)))
  (is (empty? (docs/docless-vars 'zero-one.geni.graph))))

(deftest hand-written-docstrings-stay-test
  (is (string/starts-with? (:doc (meta #'g/lit)) "Returns a column of the literal value")))
