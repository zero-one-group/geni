(ns zero-one.geni.partitioner
  (:require
   [zero-one.geni.utils :refer [import-fn]]
   [zero-one.geni.docs :as docs])
  (:import
   (org.apache.spark HashPartitioner)))

(defn hash-partitioner [partitions]
  (HashPartitioner. partitions))

(defn num-partitions [partitioner]
  (.numPartitions partitioner))

(defn get-partition [partitioner k]
  (.getPartition partitioner k))

(defn equals [left right]
  (.equals left right))

(defn hash-code [partitioner]
  (.hashCode partitioner))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.partitioner
 [(-> docs/spark-docs :methods :hash-partitioner)])

;; Aliases
(import-fn equals equals?)

