(ns zero-one.geni.ml.fpm
  (:require
    [potemkin :refer [import-fn]]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.fpm FPGrowth
                             PrefixSpan)))

(defn fp-growth [params]
  (let [defaults {:items-col      "items",
                  :min-confidence 0.8,
                  :min-support    0.3,
                  :prediction-col "prediction"}
        props     (merge defaults params)]
    (interop/instantiate FPGrowth props)))

(defn prefix-span [params]
  (let [defaults {:min-support            0.1,
                  :sequence-col           "sequence",
                  :max-pattern-length     10,
                  :max-local-proj-db-size 32000000}
        props     (merge defaults params)]
    (interop/instantiate PrefixSpan props)))

;; Docs
(docs/alter-docs-in-ns!
  'zero-one.geni.ml.fpm
  [(-> docs/spark-docs :classes :ml :fpm)])

;; Aliases
(import-fn fp-growth frequent-pattern-growth)
