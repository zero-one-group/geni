(ns zero-one.geni.docs-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.docs :as docs]))

(defn some-docless-fn [])

(defn some-fn-with-doc
  "some dummy doc"
  [])

(defn some-fn-with-invalid-doc
  [])
(docs/add-doc!
  (var some-fn-with-invalid-doc)
  ["This doc is not a string."])

(fact "Correct docless vars identification" :docs
  (docs/docless-vars 'zero-one.geni.docs-test) => [(var some-docless-fn)]
  (docs/invalid-doc-vars 'zero-one.geni.docs-test)
  => {'some-fn-with-invalid-doc (var some-fn-with-invalid-doc)})
