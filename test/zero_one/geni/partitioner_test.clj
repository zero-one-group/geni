(ns zero-one.geni.partitioner-test
  (:require
   [midje.sweet :refer [facts =>]]
   [zero-one.geni.partitioner :as partitioner]))

(facts "On partitioner fields" :rdd
  (let [partitioner (partitioner/hash-partitioner 12)]
    (partitioner/num-partitions partitioner) => 12
    (partitioner/get-partition partitioner 123) => int?
    (partitioner/equals? partitioner partitioner) => true
    (partitioner/hash-code partitioner) => int?))

