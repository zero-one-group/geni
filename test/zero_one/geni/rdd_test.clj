(ns ^:classic zero-one.geni.rdd-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.aot-functions :as aot]
   [zero-one.geni.defaults]
   [zero-one.geni.partitioner :as partitioner]
   [zero-one.geni.rdd :as rdd]
   [zero-one.geni.test-resources :refer [create-temp-file! checkpoint-dir!]])
  (:import
   (org.apache.spark SparkContext)
   (org.apache.spark.api.java JavaRDD JavaSparkContext)))

(def dummy-rdd
  (rdd/text-file "test/resources/rdd.txt"))

(def dummy-pair-rdd
  (rdd/map-to-pair dummy-rdd aot/to-pair))

(deftest ^:rdd variadic-functions-test
  (testing "expected 0-adic and 1-adic returns"
    (doall
     (for [variadic-fn [rdd/cartesian rdd/union rdd/intersection rdd/subtract]]
       (do
         (is (rdd/empty? (variadic-fn)))
         (let [rand-num (rand-int 100)]
           (is (= [rand-num]
                  (-> (rdd/parallelise [rand-num])
                      variadic-fn
                      rdd/collect))))))))
  (testing "expected 3-adic returns"
    (let [left  (rdd/parallelise [1 2 3])
          mid   (rdd/parallelise [3 4 5])
          right (rdd/parallelise [1 4 3])]
      (is (= [1 2 3 3 4 5 1 4 3] (rdd/collect (rdd/union left mid right))))
      (is (= [3] (rdd/collect (rdd/intersection left mid right))))
      (is (= 27 (rdd/count (rdd/cartesian left mid right))))
      (is (= [2] (rdd/collect (rdd/subtract left mid right))))
      (is (empty? (rdd/collect (rdd/subtract left mid right (rdd/parallelise [2]))))))))

(deftest ^:rdd javasparkcontext-methods-test
  (testing "expected static fields"
    (is (= "Geni App" (rdd/app-name)))
    (is (= [1 2 3] (rdd/value (rdd/broadcast [1 2 3]))))
    (is (string/includes? (do (checkpoint-dir!) (rdd/checkpoint-dir)) "target/checkpoint"))
    (is (map? (rdd/conf)))
    (is (integer? (rdd/default-min-partitions)))
    (is (integer? (rdd/default-parallelism)))
    (is (instance? JavaRDD (rdd/empty-rdd)))
    (is (vector? (rdd/jars)))
    (is (rdd/local?))
    (is (nil? (rdd/local-property "abc")))
    (is (= "local[*]" (rdd/master)))
    (is (map? (rdd/persistent-rdds)))
    (is (= {} (rdd/resources)))
    (is (= (System/getenv "SPARK_HOME") (rdd/spark-home)))
    (is (instance? SparkContext (rdd/sc)))
    (is (= (.version @zero-one.geni.defaults/spark) (rdd/version)))))

(deftest ^:rdd repartitioning-test
  (testing "partition-by works"
    (is (= 11
           (-> dummy-rdd
               (rdd/map-to-pair aot/to-pair)
               (rdd/partition-by (partitioner/hash-partitioner 11))
               rdd/num-partitions))))
  (testing "repartition-and-sort-within-partitions works"
    (let [actual (-> dummy-rdd
                     (rdd/map-to-pair aot/to-pair)
                     (rdd/repartition-and-sort-within-partitions (partitioner/hash-partitioner 1))
                     rdd/collect
                     distinct)]
      (is (= actual (sort actual))))
    (let [actual (-> (rdd/parallelise [1 2 3 4 5 4 3 2 1])
                     (rdd/map-to-pair aot/to-pair)
                     (rdd/repartition-and-sort-within-partitions (partitioner/hash-partitioner 1) >)
                     rdd/collect
                     distinct)]
      (is (= actual (reverse (sort actual)))))))

(deftest ^:rdd basic-pairrdd-transformations-test
  (testing "cogroup work"
    (let [left  (rdd/flat-map-to-pair dummy-rdd aot/split-spaces-and-pair)
          mid   (rdd/filter left aot/first-equals-lewis-or-carroll)
          right (rdd/filter left aot/first-equals-lewis)]
      (is (every? (-> (rdd/cogroup left mid right)
                      rdd/collect
                      flatten
                      set) [1 "eBook" "Wonderland"]))
      (is (= 4
             (-> (rdd/cogroup left mid right 4)
                 rdd/num-partitions)))
      (is (= 2
             (-> (rdd/cogroup mid right)
                 rdd/collect
                 rdd/count)))))
  (testing "sample-by-key + sample-by-key-exact works"
    (let [fractions {"Alice’s Adventures in Wonderland" 0.1
                     "Project Gutenberg’s" 0.1
                     "This eBook is for the use" 0.1
                     "at no cost and with" 0.1
                     "by Lewis Carroll" 0.1
                     "of anyone anywhere" 0.1}]
      ;; Without a seed, the count is random: about Poisson(0.5 × 126 = 63),
      ;; so (20, 120) fails less than once in 10^9 runs. With 0.1 and (2, 27),
      ;; it failed about once in 1,700.
      (is (< 20 (-> dummy-pair-rdd
                    (rdd/sample-by-key true (update-vals fractions (constantly 0.5)))
                    rdd/count) 120))
      (is (< 2 (-> dummy-pair-rdd
                   (rdd/sample-by-key true fractions 123)
                   rdd/count) 27))
      (is (= 14
             (-> dummy-pair-rdd
                 (rdd/sample-by-key-exact true fractions)
                 rdd/count)))
      (is (= 14
             (-> dummy-pair-rdd
                 (rdd/sample-by-key-exact true fractions 123)
                 rdd/count)))))
  (testing "reduce-by-key-locally works"
    (is (= {"Alice’s Adventures in Wonderland" 18
            "Project Gutenberg’s" 9
            "This eBook is for the use" 27
            "at no cost and with" 27
            "by Lewis Carroll" 18
            "of anyone anywhere" 27}
           (-> dummy-pair-rdd
               (rdd/reduce-by-key-locally +)))))
  (testing "reduce-by-key works"
    (is (= 2 (-> dummy-pair-rdd (rdd/reduce-by-key + 2) rdd/num-partitions))))
  (testing "count-by-key-approx works"
    (let [ks (let [result (-> dummy-pair-rdd
                              (rdd/count-by-key-approx 100)
                              rdd/final-value)]
               (map (comp keys second) result))]
      (is (every? #(= [:mean :confidence :low :high] %) ks))))
  (testing "count-approx-distinct-by-key works"
    (is (= [["Alice’s Adventures in Wonderland" 1]
            ["at no cost and with" 1]
            ["of anyone anywhere" 1]
            ["by Lewis Carroll" 1]
            ["Project Gutenberg’s" 1]
            ["This eBook is for the use" 1]]
           (-> dummy-pair-rdd
               (rdd/count-approx-distinct-by-key 0.01)
               rdd/collect)))
    (is (= 3
           (-> dummy-pair-rdd
               (rdd/count-approx-distinct-by-key 0.01 3)
               rdd/num-partitions))))
  (testing "combine-by-key works"
    (is (= [["Alice’s Adventures in Wonderland" "111111111111111111"]
            ["at no cost and with" "111111111111111111111111111"]
            ["of anyone anywhere" "111111111111111111111111111"]
            ["by Lewis Carroll" "111111111111111111"]
            ["Project Gutenberg’s" "111111111"]
            ["This eBook is for the use" "111111111111111111111111111"]]
           (-> dummy-pair-rdd
               (rdd/combine-by-key str str str)
               rdd/collect)))
    (is (= 2
           (-> dummy-pair-rdd
               (rdd/combine-by-key str str str 2)
               rdd/num-partitions))))
  (testing "fold-by-key works"
    (is (= [["Alice’s Adventures in Wonderland" -2]
            ["at no cost and with" 1]
            ["of anyone anywhere" 1]
            ["by Lewis Carroll" 0]
            ["Project Gutenberg’s" -1]
            ["This eBook is for the use" 1]]
           (-> dummy-pair-rdd
               (rdd/fold-by-key 100 -)
               rdd/collect)))
    (is (= 2
           (-> dummy-pair-rdd
               (rdd/fold-by-key 0 2 -)
               rdd/num-partitions))))
  (testing "aggregate-by-key works"
    (is (= [["Alice’s Adventures in Wonderland" 18]
            ["at no cost and with" 27]
            ["of anyone anywhere" 27]
            ["by Lewis Carroll" 18]
            ["Project Gutenberg’s" 9]
            ["This eBook is for the use" 27]]
           (-> dummy-pair-rdd
               (rdd/aggregate-by-key 0 + +)
               rdd/collect)))
    (is (= 3
           (-> dummy-pair-rdd
               (rdd/aggregate-by-key 3 0 + +)
               rdd/num-partitions))))
  (testing "group-by works"
    (is (= ["(Alice’s Adventures in Wonderland,1)"
            "(of anyone anywhere,1)"
            "(Project Gutenberg’s,1)"
            "(by Lewis Carroll,1)"
            "(at no cost and with,1)"
            "(This eBook is for the use,1)"]
           (-> dummy-pair-rdd
               (rdd/group-by str)
               rdd/keys
               rdd/distinct
               rdd/collect)))
    (is (= 7 (-> dummy-pair-rdd (rdd/group-by str 7) rdd/num-partitions)))
    (is (string/includes? (-> dummy-pair-rdd (rdd/group-by str 11) rdd/name) "[clojure.core/str, 11]")))
  (testing "count-by-key works"
    (is (= {"Alice’s Adventures in Wonderland" 18
            "Project Gutenberg’s" 9
            "This eBook is for the use" 27
            "at no cost and with" 27
            "by Lewis Carroll" 18
            "of anyone anywhere" 27}
           (rdd/count-by-key dummy-pair-rdd))))
  (testing "lookup works"
    (is (= [1]
           (-> dummy-pair-rdd
               (rdd/lookup "at no cost and with")
               distinct))))
  (testing "map-values works"
    (is (= [2]
           (-> dummy-pair-rdd
               (rdd/map-values inc)
               rdd/values
               rdd/distinct
               rdd/collect))))
  (testing "flat-map-values works"
    (is (= #{["at no cost and with" 1]
             ["by Lewis Carroll" 1]
             ["Alice’s Adventures in Wonderland" 1]
             ["of anyone anywhere" 1]
             ["This eBook is for the use" 1]
             ["Project Gutenberg’s" 1]}
           (-> dummy-pair-rdd
               (rdd/flat-map-values aot/to-pair)
               rdd/distinct
               rdd/collect
               set))))
  (testing "keys + values work"
    (is (= 6 (-> dummy-pair-rdd rdd/keys rdd/distinct rdd/count)))
    (is (= [1] (-> dummy-pair-rdd rdd/values rdd/distinct rdd/collect))))
  (testing "filter + join + subtract-by-key work"
    (let [left  (rdd/flat-map-to-pair dummy-rdd aot/split-spaces-and-pair)
          right (rdd/filter left aot/first-equals-lewis)]
      (is (= [["Lewis" 1]] (-> right rdd/distinct rdd/collect)))
      (is (= [["Lewis" [1 1]]] (-> left (rdd/join right) rdd/distinct rdd/collect)))
      (is (= 324 (-> left (rdd/right-outer-join right) rdd/count)))
      (is (= 828 (-> left (rdd/left-outer-join right) rdd/count)))
      (is (= 828 (-> left (rdd/full-outer-join right) rdd/count)))
      (is (= 11 (-> left (rdd/join right 11) rdd/num-partitions)))
      (is (= 2 (-> left (rdd/right-outer-join right 2) rdd/num-partitions)))
      (is (= 3 (-> left (rdd/left-outer-join right 3) rdd/num-partitions)))
      (is (= 4 (-> left (rdd/full-outer-join right 4) rdd/num-partitions)))
      (is (= 22 (-> left (rdd/subtract-by-key right) rdd/distinct rdd/count)))
      (is (= 4 (-> left (rdd/subtract-by-key right 4) rdd/num-partitions))))))

(deftest ^:rdd basic-rdd-saving-and-loading-test
  (testing "binary-files works"
    (is (= 1 (rdd/count (rdd/binary-files "test/resources/housing.parquet/*.parquet"))))
    (is (= 1
           (rdd/count
            (rdd/binary-files "test/resources/housing.parquet/*.parquet" 2)))))
  (testing "save-as-text-file works"
    (let [write-rdd (rdd/parallelise (mapv (fn [_] (rand-int 100)) (range 100)))
          temp-file (create-temp-file! ".rdd")
          read-rdd  (do
                      (io/delete-file temp-file true)
                      (rdd/save-as-text-file write-rdd (str temp-file))
                      (rdd/text-file (str temp-file)))]
      (is (= (rdd/count write-rdd) (rdd/count read-rdd)))
      (is (pos? (rdd/count (rdd/whole-text-files (str temp-file)))))
      (is (< 1 (rdd/count (rdd/whole-text-files (str temp-file) 2)))))))

(deftest ^:rdd basic-rdd-fields-test
  (let [rdd (rdd/parallelise-doubles [1])]
    (is (instance? JavaSparkContext (rdd/context rdd)))
    (is (integer? (rdd/id rdd)))
    (is (nil? (rdd/name rdd)))
    (is (not (rdd/checkpointed? rdd)))
    (is (rdd/empty? (rdd/parallelise [])))
    (is (not (rdd/empty? rdd)))
    (is (not (rdd/empty? rdd)))
    (is (nil? (rdd/partitioner rdd)))
    (is (not (nil? (-> dummy-rdd
                       (rdd/map-to-pair aot/to-pair)
                       (rdd/group-by-key (partitioner/hash-partitioner 123))
                       rdd/partitioner))))))

(deftest ^:rdd basic-partialresult-test
  (let [result (rdd/count-approx dummy-rdd 1000)]
    (is (every? (rdd/initial-value result) [:mean :low :high :confidence]))
    (is (every? (rdd/final-value result) [:mean :low :high :confidence]))
    (is (boolean? (rdd/final? result))))
  (is (< 100 (-> (rdd/count-approx dummy-rdd 1000 0.9) rdd/initial-value :low))))

(deftest ^:rdd basic-rdd-actions-test
  (testing "collect-async works"
    (is (= [1] @(rdd/collect-async (rdd/parallelise [1])))))
  (testing "collect-partitions works"
    (let [actual (let [rdd     (rdd/parallelise (into [] (range 100)))
                       part-id (->> rdd rdd/partitions (map #(.index %)) first)]
                   (rdd/collect-partitions rdd [part-id]))]
      (is (and (every? seq? actual)
               (every? (set (range 100)) (flatten actual))))))
  (testing "count-approx-distinct works"
    (is (< 3 (rdd/count-approx-distinct dummy-rdd 0.01) 7)))
  (testing "count-async works"
    (is (= 126 @(rdd/count-async dummy-rdd))))
  (testing "count-by-value works"
    (is (= {"Alice’s Adventures in Wonderland" 18
            "Project Gutenberg’s" 9
            "This eBook is for the use" 27
            "at no cost and with" 27
            "by Lewis Carroll" 18
            "of anyone anywhere" 27}
           (rdd/count-by-value dummy-rdd))))
  (testing "first works"
    (is (= "Project Gutenberg’s" (rdd/first dummy-rdd))))
  (testing "foreach works"
    (is (nil? (rdd/foreach dummy-rdd identity))))
  (testing "foreach-async works"
    (is (nil? @(rdd/foreach-async dummy-rdd identity))))
  (testing "foreach-partition works"
    (is (nil? (rdd/foreach-partition dummy-rdd identity))))
  (testing "foreach-partition-async works"
    (is (nil? @(rdd/foreach-partition-async dummy-rdd identity))))
  (testing "take works"
    (is (= ["Project Gutenberg’s"
            "Alice’s Adventures in Wonderland"
            "by Lewis Carroll"]
           (rdd/take dummy-rdd 3))))
  (testing "take-async works"
    (is (= ["Project Gutenberg’s"
            "Alice’s Adventures in Wonderland"]
           @(rdd/take-async dummy-rdd 2))))
  (testing "take-ordered works"
    (let [actual (rdd/take-ordered dummy-rdd 20)]
      (is (= (sort actual) actual)))
    (let [rdd    (rdd/parallelise (mapv (fn [_] (rand-int 100)) (range 100)))
          actual (rdd/take-ordered rdd 20 >)]
      (is (= (sort actual) (reverse actual)))))
  (testing "take-sample works"
    (let [rdd (rdd/parallelise (into [] (range 100)))]
      (is (= (-> (rdd/take-sample rdd false 10) distinct count) 10)))
    (let [rdd (rdd/parallelise (into [] (range 100)))]
      (is (< (-> (rdd/take-sample rdd true 100 1) distinct count) 100)))))

(deftest ^:rdd basic-rdd-transformations-actions-test
  (is (= ["of anyone anywhere" "of anyone anywhere"] (-> dummy-rdd (rdd/top 2))))
  (is (= [3 2]
         (-> (rdd/parallelise [1 2 3])
             (rdd/top 2 <))))
  (is (< 1 (-> (rdd/text-file "test/resources/rdd.txt" 2)
               (rdd/map-to-pair aot/to-pair)
               rdd/group-by-key
               rdd/num-partitions)))
  (is (= [[1 2] [3 4]]
         (-> (rdd/parallelise-pairs [[1 2] [3 4]])
             rdd/collect)))
  (is (= 7
         (-> dummy-pair-rdd
             (rdd/group-by-key 7)
             rdd/num-partitions)))
  (testing "aggregate and fold work"
    (is (= 45
           (-> (rdd/parallelise (range 10))
               (rdd/aggregate 0 + +))))
    (is (= 45
           (-> (rdd/parallelise (range 10))
               (rdd/fold 0 +)))))
  (testing "subtract works"
    (let [left (rdd/parallelise [1 2 3 4 5])
          right (rdd/parallelise [9 8 7 6 5])]
      (is (= #{1 2 3 4} (-> (rdd/subtract left right) rdd/collect set)))
      (is (= 3 (rdd/num-partitions (rdd/subtract left right 3))))))
  (testing "random-split works"
    (let [actual (->> (rdd/random-split dummy-rdd [0.9 0.1])
                      (map rdd/count))]
      (is (< (second actual) (first actual))))
    (let [actual (->> (rdd/random-split dummy-rdd [0.1 0.9] 123)
                      (map rdd/count))]
      (is (< (first actual) (second actual)))))
  (testing "persist and unpersist work"
    (is (= rdd/disk-only
           (-> (rdd/parallelise [1])
               (rdd/persist rdd/disk-only)
               rdd/storage-level)))
    (is (not= (-> (rdd/parallelise [1])
                  (rdd/persist rdd/disk-only)
                  rdd/unpersist
                  rdd/storage-level) rdd/disk-only))
    (is (not= (-> (rdd/parallelise [1])
                  (rdd/persist rdd/disk-only)
                  (rdd/unpersist false)
                  rdd/storage-level) rdd/disk-only)))
  (testing "max and min work"
    (is (= 3 (-> (rdd/parallelise [-1 2 3]) (rdd/max <))))
    (is (= 3 (-> (rdd/parallelise [-1 2 3]) (rdd/min >)))))
  (testing "key-by works"
    (is (= [["a" "a"] ["b" "b"] ["c" "c"]]
           (-> (rdd/parallelise ["a" "b" "c"])
               (rdd/key-by identity)
               rdd/collect))))
  (testing "flat-map + filter works"
    (let [result-rdd (-> dummy-rdd
                         (rdd/flat-map aot/split-spaces)
                         (rdd/filter aot/equals-lewis))]
      (is (= 18 (-> result-rdd rdd/collect count)))
      (is (not (nil? (-> result-rdd rdd/name))))))
  (testing "map works"
    (is (every? integer? (-> dummy-rdd
                             (rdd/map count)
                             rdd/collect))))
  (testing "reduce works"
    (is (= 2709
           (-> dummy-rdd
               (rdd/map count)
               (rdd/reduce +))))
    (is (= 120
           (-> (rdd/parallelise [1 2 3 4 5])
               (rdd/reduce *)))))
  (testing "map-to-pair + reduce-by-key + collect work"
    (is (= [["Alice’s Adventures in Wonderland" 18]
            ["at no cost and with" 27]
            ["of anyone anywhere" 27]
            ["by Lewis Carroll" 18]
            ["Project Gutenberg’s" 9]
            ["This eBook is for the use" 27]]
           (-> dummy-pair-rdd
               (rdd/reduce-by-key +)
               rdd/collect)))
    (let [actual (-> dummy-pair-rdd
                     rdd/collect)]
      (is (and (every? vector? actual)
               (every? (comp (partial = 2) count) actual)
               (every? (comp string? first) actual)
               (every? (comp (partial = 1) second) actual)))))
  (testing "sort-by-key works"
    (let [actual (-> dummy-pair-rdd
                     (rdd/reduce-by-key +)
                     rdd/sort-by-key
                     rdd/collect)]
      (is (= (sort actual) actual)))
    (let [actual (-> dummy-pair-rdd
                     (rdd/reduce-by-key +)
                     (rdd/sort-by-key false)
                     rdd/collect)]
      (is (= (sort actual) (reverse actual)))))
  (testing "flat-map-to-pair works"
    (is (= #{["spark" 2] ["world" 1] ["and" 1] ["geni!" 1] ["the" 1]
             ["awesome!" 1] ["is" 1] ["hello" 2] ["world!" 1]}
           (-> (rdd/parallelise ["hello world!"
                                 "hello spark and geni!"
                                 "the spark world is awesome!"])
               (rdd/flat-map-to-pair aot/split-spaces-and-pair)
               (rdd/reduce-by-key +)
               rdd/collect
               set))))
  (testing "map-partitions works"
    (is (= ["abc" "def" "ghi" "jkl" "mno" "pqr"]
           (-> (rdd/parallelise ["abc def" "ghi jkl" "mno pqr"])
               (rdd/map-partitions aot/map-split-spaces)
               rdd/collect))))
  (testing "map-partitions-to-pair works"
    (is (= [["abc" 1] ["def" 1]]
           (-> (rdd/parallelise ["abc def"])
               (rdd/map-partitions-to-pair aot/mapcat-split-spaces)
               rdd/collect)))
    (is (= (rdd/default-parallelism)
           (-> (rdd/parallelise ["abc def"])
               (rdd/map-partitions-to-pair aot/mapcat-split-spaces true)
               rdd/num-partitions))))
  (testing "map-partitions-with-index works"
    (let [actual (-> (rdd/parallelise ["abc def" "ghi jkl" "mno pqr"])
                     (rdd/map-partitions-with-index aot/map-split-spaces-with-index)
                     rdd/collect)]
      (is (and (every? integer? (map first actual))
               (= (set (map second actual))
                  #{"abc" "def" "ghi" "jkl" "mno" "pqr"})))))
  (testing "zips work"
    (let [left (rdd/parallelise ["a b c" "d e f g h i"])
          right (rdd/parallelise ["j k l m n o" "pqr stu"])]
      (is (= [["a b c" "j k l m n o"] ["d e f g h i" "pqr stu"]]
             (-> (rdd/zip left right)
                 rdd/collect)))
      (is (= ["aj" "bk" "cl" "dpqr" "estu"]
             (-> (rdd/zip-partitions left right aot/zip-split-spaces)
                 rdd/collect)))
      (is (= [["a b c" 0] ["d e f g h i" 1]]
             (-> (rdd/zip-with-index left)
                 rdd/collect))))
    (let [zipped-values (rdd/collect (rdd/zip-with-unique-id dummy-rdd))]
      (is (= (rdd/count dummy-rdd) (->> zipped-values (map second) set count)))))
  (testing "sample works"
    (let [rdd dummy-rdd]
      (is (< 2 (rdd/count (rdd/sample rdd true 0.1 123)) 27))
      (is (< 2 (rdd/count (rdd/sample rdd false 0.1 123)) 27))))
  (testing "coalesce works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (is (= ["abc" "def"] (-> rdd (rdd/coalesce 1) rdd/collect)))
      (is (= #{"abc" "def"} (-> rdd (rdd/coalesce 1 true) rdd/collect set)))))
  (testing "repartition works"
    (is (= 10 (-> dummy-rdd (rdd/repartition 10) rdd/num-partitions))))
  (testing "cartesian works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (is (= [["abc" "def"] ["abc" "ghi"] ["def" "def"] ["def" "ghi"]] (rdd/collect (rdd/cartesian left right))))))
  (testing "cache works"
    (is (= 126 (-> dummy-rdd rdd/cache rdd/count))))
  (testing "distinct works"
    (is (= 6 (-> dummy-rdd rdd/distinct rdd/collect count)))
    (is (= 2 (-> dummy-rdd (rdd/distinct 2) rdd/num-partitions)))
    (is (string/includes? (-> dummy-rdd (rdd/distinct 3) rdd/name) "[3]")))
  (testing "zip-partitions works"
    (is (= ["aj" "bk" "cl" "dpqr" "estu"]
           (let [left (rdd/parallelise ["a b c" "d e f g h i"])
                 right (rdd/parallelise ["j k l m n o" "pqr stu"])]
             (-> (rdd/zip-partitions left right aot/zip-split-spaces)
                 rdd/collect)))))
  (testing "union works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (is (= ["abc" "def" "abc" "def"] (rdd/collect (rdd/union rdd rdd))))))
  (testing "intersection works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (is (= ["def"] (rdd/collect (rdd/intersection left right))))))
  (testing "glom works"
    (is (< (-> dummy-rdd rdd/glom rdd/count) 126))))
