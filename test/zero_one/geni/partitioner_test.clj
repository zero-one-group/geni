(ns zero-one.geni.partitioner-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.partitioner :as partitioner]))

(deftest ^:rdd partitioner-fields-test
  (let [partitioner (partitioner/hash-partitioner 12)]
    (is (= 12 (partitioner/num-partitions partitioner)))
    (is (int? (partitioner/get-partition partitioner 123)))
    (is (partitioner/equals? partitioner partitioner))
    (is (int? (partitioner/hash-code partitioner)))))

