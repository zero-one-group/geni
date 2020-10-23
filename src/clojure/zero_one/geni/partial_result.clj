(ns zero-one.geni.partial-result
  (:require
   [potemkin :refer [import-fn]]
   [zero-one.geni.docs :as docs]))

(defn get-final-value [result] (.getFinalValue result))

(defn initial-value [result] (.initialValue result))

(defn is-initial-value-final [result] (.isInitialValueFinal result))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.partial-result
 [(-> docs/spark-docs :methods :partial-result)])

;; Aliases
(import-fn get-final-value final-value)
(import-fn is-initial-value-final final?)
