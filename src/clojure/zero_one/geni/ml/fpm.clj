(ns zero-one.geni.ml.fpm
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]]))

(interop/def-stages org.apache.spark.ml.fpm
  [fp-growth FPGrowth]
  [prefix-span PrefixSpan])

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.fpm
 [(-> docs/spark-docs :classes :ml :fpm)])

;; Aliases
(import-fn fp-growth frequent-pattern-growth)
