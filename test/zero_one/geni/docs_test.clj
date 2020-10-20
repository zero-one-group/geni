(ns zero-one.geni.docs-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.main]
    [zero-one.geni.ml]
    [zero-one.geni.rdd]
    [zero-one.geni.repl]))

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

(fact "Frequently required namespaces must have complete docs" :docs
  (docs/docless-vars 'zero-one.geni.core) => empty?
  (docs/docless-vars 'zero-one.geni.main) => empty?
  (docs/docless-vars 'zero-one.geni.ml) => empty?
  (docs/docless-vars 'zero-one.geni.rdd) => empty?
  (docs/docless-vars 'zero-one.geni.repl) => empty?)
