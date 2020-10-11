(ns zero-one.geni.ml.fpm
  (:require
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
(def frequent-pattern-growth fp-growth)

(defn prefix-span [params]
  (let [defaults {:min-support            0.1,
                  :sequence-col           "sequence",
                  :max-pattern-length     10,
                  :max-local-proj-db-size 32000000}
        props     (merge defaults params)]
    (interop/instantiate PrefixSpan props)))
