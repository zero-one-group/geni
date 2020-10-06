(ns zero-one.geni.docs-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.docs :as docs]))

(defn some-docless-fn [])

(defn some-fn-with-doc
  "some dummy doc"
  [])

(fact "Correct docless vars identification" :docs
  (docs/docless-vars 'zero-one.geni.docs-test) => [(var some-docless-fn)])
