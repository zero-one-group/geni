(ns zero-one.geni.partitioner
  (:refer-clojure :exclude [partition])
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
(def equals? equals)

(defn hash-code [partitioner]
  (.hashCode partitioner))
