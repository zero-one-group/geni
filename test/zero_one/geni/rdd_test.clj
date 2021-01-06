(ns zero-one.geni.rdd-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.aot-functions :as aot]
   [zero-one.geni.defaults]
   [zero-one.geni.partitioner :as partitioner]
   [zero-one.geni.rdd :as rdd]
   [zero-one.geni.test-resources :refer [create-temp-file!]])
  (:import
   (org.apache.spark SparkContext)
   (org.apache.spark.api.java JavaRDD JavaSparkContext)))

(def dummy-rdd
  (rdd/text-file "test/resources/rdd.txt"))

(def dummy-pair-rdd
  (rdd/map-to-pair dummy-rdd aot/to-pair))

(facts "On variadic functions" :rdd
  (fact "expected 0-adic and 1-adic returns"
    (doall
     (for [variadic-fn [rdd/cartesian rdd/union rdd/intersection rdd/subtract]]
       (do
         (variadic-fn) => rdd/empty?
         (let [rand-num (rand-int 100)]
           (-> (rdd/parallelise [rand-num])
               variadic-fn
               rdd/collect) => [rand-num])))))
  (fact "expected 3-adic returns"
    (let [left  (rdd/parallelise [1 2 3])
          mid   (rdd/parallelise [3 4 5])
          right (rdd/parallelise [1 4 3])]
      (rdd/collect (rdd/union left mid right)) => [1 2 3 3 4 5 1 4 3]
      (rdd/collect (rdd/intersection left mid right)) => [3]
      (rdd/count (rdd/cartesian left mid right)) => 27
      (rdd/collect (rdd/subtract left mid right)) => [2]
      (rdd/collect (rdd/subtract left mid right (rdd/parallelise [2]))) => empty?)))

(facts "On JavaSparkContext methods" :rdd
  (fact "expected static fields"
    (rdd/app-name) => "Geni App"
    (rdd/value (rdd/broadcast [1 2 3])) => [1 2 3]
    (rdd/checkpoint-dir) => string?
    (rdd/conf) => map?
    (rdd/default-min-partitions) => integer?
    (rdd/default-parallelism) => integer?
    (rdd/empty-rdd) => (partial instance? JavaRDD)
    (rdd/jars) => vector?
    (rdd/local?) => true
    (rdd/local-property "abc") => nil?
    (rdd/master) => "local[*]"
    (rdd/persistent-rdds) => map?
    (rdd/resources) => {}
    (rdd/spark-home) => (System/getenv "SPARK_HOME")
    (rdd/sc) => (partial instance? SparkContext)
    (rdd/version) => "3.1.0"))

(facts "On repartitioning" :rdd
  (fact "partition-by works"
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        (rdd/partition-by (partitioner/hash-partitioner 11))
        rdd/num-partitions) => 11)
  (fact "repartition-and-sort-within-partitions works"
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        (rdd/repartition-and-sort-within-partitions (partitioner/hash-partitioner 1))
        rdd/collect
        distinct) => #(= % (sort %))
    (-> (rdd/parallelise [1 2 3 4 5 4 3 2 1])
        (rdd/map-to-pair aot/to-pair)
        (rdd/repartition-and-sort-within-partitions (partitioner/hash-partitioner 1) >)
        rdd/collect
        distinct) => #(= % (reverse (sort %)))))

(facts "On basic PairRDD transformations" :rdd
  (fact "cogroup work"
    (let [left  (rdd/flat-map-to-pair dummy-rdd aot/split-spaces-and-pair)
          mid   (rdd/filter left aot/first-equals-lewis-or-carroll)
          right (rdd/filter left aot/first-equals-lewis)]
      (-> (rdd/cogroup left mid right)
          rdd/collect
          flatten
          set) => #(every? % [1 "eBook" "Wonderland"])
      (-> (rdd/cogroup left mid right 4)
          rdd/num-partitions) => 4
      (-> (rdd/cogroup mid right)
          rdd/collect
          rdd/count) => 2))
  (fact "sample-by-key + sample-by-key-exact works"
    (let [fractions {"Alice’s Adventures in Wonderland" 0.1
                     "Project Gutenberg’s" 0.1
                     "This eBook is for the use" 0.1
                     "at no cost and with" 0.1
                     "by Lewis Carroll" 0.1
                     "of anyone anywhere" 0.1}]
      (-> dummy-pair-rdd
          (rdd/sample-by-key true fractions)
          rdd/count) => #(< 2 % 27)
      (-> dummy-pair-rdd
          (rdd/sample-by-key true fractions 123)
          rdd/count) => #(< 2 % 27)
      (-> dummy-pair-rdd
          (rdd/sample-by-key-exact true fractions)
          rdd/count) => 14
      (-> dummy-pair-rdd
          (rdd/sample-by-key-exact true fractions 123)
          rdd/count) => 14))
  (fact "reduce-by-key-locally works"
    (-> dummy-pair-rdd
        (rdd/reduce-by-key-locally +)) => {"Alice’s Adventures in Wonderland" 18
                                           "Project Gutenberg’s" 9
                                           "This eBook is for the use" 27
                                           "at no cost and with" 27
                                           "by Lewis Carroll" 18
                                           "of anyone anywhere" 27})
  (fact "reduce-by-key works"
    (-> dummy-pair-rdd (rdd/reduce-by-key + 2) rdd/num-partitions) => 2)
  (fact "count-by-key-approx works"
    (let [result (-> dummy-pair-rdd
                     (rdd/count-by-key-approx 100)
                     rdd/final-value)]
      (map (comp keys second) result))
    => (fn [ks] (every? #(= [:mean :confidence :low :high] %) ks)))
  (fact "count-approx-distinct-by-key works"
    (-> dummy-pair-rdd
        (rdd/count-approx-distinct-by-key 0.01)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 1]
                         ["at no cost and with" 1]
                         ["of anyone anywhere" 1]
                         ["by Lewis Carroll" 1]
                         ["Project Gutenberg’s" 1]
                         ["This eBook is for the use" 1]]
    (-> dummy-pair-rdd
        (rdd/count-approx-distinct-by-key 0.01 3)
        rdd/num-partitions) => 3)
  (fact "combine-by-key works"
    (-> dummy-pair-rdd
        (rdd/combine-by-key str str str)
        rdd/collect) => [["Alice’s Adventures in Wonderland" "111111111111111111"]
                         ["at no cost and with" "111111111111111111111111111"]
                         ["of anyone anywhere" "111111111111111111111111111"]
                         ["by Lewis Carroll" "111111111111111111"]
                         ["Project Gutenberg’s" "111111111"]
                         ["This eBook is for the use" "111111111111111111111111111"]]
    (-> dummy-pair-rdd
        (rdd/combine-by-key str str str 2)
        rdd/num-partitions) => 2)
  (fact "fold-by-key works"
    (-> dummy-pair-rdd
        (rdd/fold-by-key 100 -)
        rdd/collect) => [["Alice’s Adventures in Wonderland" -2]
                         ["at no cost and with" 1]
                         ["of anyone anywhere" 1]
                         ["by Lewis Carroll" 0]
                         ["Project Gutenberg’s" -1]
                         ["This eBook is for the use" 1]]
    (-> dummy-pair-rdd
        (rdd/fold-by-key 0 2 -)
        rdd/num-partitions) => 2)
  (fact "aggregate-by-key works"
    (-> dummy-pair-rdd
        (rdd/aggregate-by-key 0 + +)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 18]
                         ["at no cost and with" 27]
                         ["of anyone anywhere" 27]
                         ["by Lewis Carroll" 18]
                         ["Project Gutenberg’s" 9]
                         ["This eBook is for the use" 27]]
    (-> dummy-pair-rdd
        (rdd/aggregate-by-key 3 0 + +)
        rdd/num-partitions) => 3)
  (fact "group-by works"
    (-> dummy-pair-rdd
        (rdd/group-by str)
        rdd/keys
        rdd/distinct
        rdd/collect) => ["(Alice’s Adventures in Wonderland,1)"
                         "(of anyone anywhere,1)"
                         "(Project Gutenberg’s,1)"
                         "(by Lewis Carroll,1)"
                         "(at no cost and with,1)"
                         "(This eBook is for the use,1)"]
    (-> dummy-pair-rdd (rdd/group-by str 7) rdd/num-partitions) => 7
    (-> dummy-pair-rdd (rdd/group-by str 11) rdd/name)
    => #(string/includes? % "[clojure.core/str, 11]"))
  (fact "count-by-key works"
    (rdd/count-by-key dummy-pair-rdd) => {"Alice’s Adventures in Wonderland" 18
                                          "Project Gutenberg’s" 9
                                          "This eBook is for the use" 27
                                          "at no cost and with" 27
                                          "by Lewis Carroll" 18
                                          "of anyone anywhere" 27})
  (fact "lookup works"
    (-> dummy-pair-rdd
        (rdd/lookup "at no cost and with")
        distinct) => [1])
  (fact "map-values works"
    (-> dummy-pair-rdd
        (rdd/map-values inc)
        rdd/values
        rdd/distinct
        rdd/collect) => [2])
  (fact "flat-map-values works"
    (-> dummy-pair-rdd
        (rdd/flat-map-values aot/to-pair)
        rdd/distinct
        rdd/collect) => [["at no cost and with" 1]
                         ["by Lewis Carroll" 1]
                         ["Alice’s Adventures in Wonderland" 1]
                         ["of anyone anywhere" 1]
                         ["This eBook is for the use" 1]
                         ["Project Gutenberg’s" 1]])
  (fact "keys + values work"
    (-> dummy-pair-rdd rdd/keys rdd/distinct rdd/count) => 6
    (-> dummy-pair-rdd rdd/values rdd/distinct rdd/collect) => [1])
  (fact "filter + join + subtract-by-key work"
    (let [left  (rdd/flat-map-to-pair dummy-rdd aot/split-spaces-and-pair)
          right (rdd/filter left aot/first-equals-lewis)]
      (-> right rdd/distinct rdd/collect) => [["Lewis" 1]]
      (-> left (rdd/join right) rdd/distinct rdd/collect) => [["Lewis" [1 1]]]
      (-> left (rdd/right-outer-join right) rdd/count) => 324
      (-> left (rdd/left-outer-join right) rdd/count) => 828
      (-> left (rdd/full-outer-join right) rdd/count) => 828
      (-> left (rdd/join right 11) rdd/num-partitions) => 11
      (-> left (rdd/right-outer-join right 2) rdd/num-partitions) => 2
      (-> left (rdd/left-outer-join right 3) rdd/num-partitions) => 3
      (-> left (rdd/full-outer-join right 4) rdd/num-partitions) => 4
      (-> left (rdd/subtract-by-key right) rdd/distinct rdd/count) => 22
      (-> left (rdd/subtract-by-key right 4) rdd/num-partitions) => 4)))

(facts "On basic RDD saving and loading" :rdd
  (fact "binary-files works"
    (rdd/count (rdd/binary-files "test/resources/housing.parquet/*.parquet")) => 1
    (rdd/count
     (rdd/binary-files "test/resources/housing.parquet/*.parquet" 2)) => 1)
  (fact "save-as-text-file works"
    (let [write-rdd (rdd/parallelise (mapv (fn [_] (rand-int 100)) (range 100)))
          temp-file (create-temp-file! ".rdd")
          read-rdd  (do
                      (io/delete-file temp-file true)
                      (rdd/save-as-text-file write-rdd (str temp-file))
                      (rdd/text-file (str temp-file)))]
      (rdd/count read-rdd) => (rdd/count write-rdd)
      (rdd/count (rdd/whole-text-files (str temp-file)))  => pos?
      (rdd/count (rdd/whole-text-files (str temp-file) 2))  => #(< 1 %))))

(facts "On basic RDD fields" :rdd
  (let [rdd (rdd/parallelise-doubles [1])]
    (rdd/context rdd) => (partial instance? JavaSparkContext)
    (rdd/id rdd) => integer?
    (rdd/name rdd) => nil?
    (rdd/checkpointed? rdd) => false
    (rdd/empty? (rdd/parallelise [])) => true
    (rdd/empty? rdd) => false
    (rdd/empty? rdd) => false
    (rdd/partitioner rdd) => nil?
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        (rdd/group-by-key (partitioner/hash-partitioner 123))
        rdd/partitioner) => (complement nil?)))

(facts "On basic PartialResult" :rdd
  (let [result (rdd/count-approx dummy-rdd 1000)]
    (rdd/initial-value result) => #(every? % [:mean :low :high :confidence])
    (rdd/final-value result) => #(every? % [:mean :low :high :confidence])
    (rdd/final? result) => boolean?)
  (-> (rdd/count-approx dummy-rdd 1000 0.9) rdd/initial-value :low) => #(< 100 %))

(facts "On basic RDD actions" :rdd
  (fact "collect-async works"
    @(rdd/collect-async (rdd/parallelise [1])) => [1])
  (fact "collect-partitions works"
    (let [rdd     (rdd/parallelise (into [] (range 100)))
          part-id (->> rdd rdd/partitions (map #(.index %)) first)]
      (rdd/collect-partitions rdd [part-id]))
    => #(and (every? seq? %)
             (every? (set (range 100)) (flatten %))))
  (fact "count-approx-distinct works"
    (rdd/count-approx-distinct dummy-rdd 0.01) => #(< 3 % 7))
  (fact "count-async works"
    @(rdd/count-async dummy-rdd) => 126)
  (fact "count-by-value works"
    (rdd/count-by-value dummy-rdd) => {"Alice’s Adventures in Wonderland" 18
                                       "Project Gutenberg’s" 9
                                       "This eBook is for the use" 27
                                       "at no cost and with" 27
                                       "by Lewis Carroll" 18
                                       "of anyone anywhere" 27})
  (fact "first works"
    (rdd/first dummy-rdd) => "Project Gutenberg’s")
  (fact "foreach works"
    (rdd/foreach dummy-rdd identity) => nil?)
  (fact "foreach-async works"
    @(rdd/foreach-async dummy-rdd identity) => nil?)
  (fact "foreach-partition works"
    (rdd/foreach-partition dummy-rdd identity) => nil?)
  (fact "foreach-partition-async works"
    @(rdd/foreach-partition-async dummy-rdd identity) => nil?)
  (fact "take works"
    (rdd/take dummy-rdd 3) => ["Project Gutenberg’s"
                               "Alice’s Adventures in Wonderland"
                               "by Lewis Carroll"])
  (fact "take-async works"
    @(rdd/take-async dummy-rdd 2) => ["Project Gutenberg’s"
                                      "Alice’s Adventures in Wonderland"])
  (fact "take-ordered works"
    (rdd/take-ordered dummy-rdd 20) => #(= (sort %) %)
    (let [rdd (rdd/parallelise (mapv (fn [_] (rand-int 100)) (range 100)))]
      (rdd/take-ordered rdd 20 >) => #(= (sort %) (reverse %))))
  (fact "take-sample works"
    (let [rdd (rdd/parallelise (into [] (range 100)))]
      (rdd/take-sample rdd false 10) => #(= (-> % distinct count) 10))
    (let [rdd (rdd/parallelise (into [] (range 100)))]
      (rdd/take-sample rdd true 100 1) => #(< (-> % distinct count) 100))))

(facts "On basic RDD transformations + actions" :rdd
  (-> dummy-rdd (rdd/top 2)) => ["of anyone anywhere" "of anyone anywhere"]
  (-> (rdd/parallelise [1 2 3])
      (rdd/top 2 <)) => [3 2]
  (-> (rdd/text-file "test/resources/rdd.txt" 2)
      (rdd/map-to-pair aot/to-pair)
      rdd/group-by-key
      rdd/num-partitions) => #(< 1 %)
  (-> (rdd/parallelise-pairs [[1 2] [3 4]])
      rdd/collect) => [[1 2] [3 4]]
  (-> dummy-pair-rdd
      (rdd/group-by-key 7)
      rdd/num-partitions) => 7
  (fact "aggregate and fold work"
    (-> (rdd/parallelise (range 10))
        (rdd/aggregate 0 + +)) => 45
    (-> (rdd/parallelise (range 10))
        (rdd/fold 0 +)) => 45)
  (fact "subtract works"
    (let [left (rdd/parallelise [1 2 3 4 5])
          right (rdd/parallelise [9 8 7 6 5])]
      (-> (rdd/subtract left right) rdd/collect set) => #{1 2 3 4}
      (rdd/num-partitions (rdd/subtract left right 3)) => 3))
  (fact "random-split works"
    (->> (rdd/random-split dummy-rdd [0.9 0.1])
         (map rdd/count)) => #(< (second %) (first %))
    (->> (rdd/random-split dummy-rdd [0.1 0.9] 123)
         (map rdd/count)) => #(< (first %) (second %)))
  (fact "persist and unpersist work"
    (-> (rdd/parallelise [1])
        (rdd/persist rdd/disk-only)
        rdd/storage-level) => rdd/disk-only
    (-> (rdd/parallelise [1])
        (rdd/persist rdd/disk-only)
        rdd/unpersist
        rdd/storage-level) => #(not= % rdd/disk-only)
    (-> (rdd/parallelise [1])
        (rdd/persist rdd/disk-only)
        (rdd/unpersist false)
        rdd/storage-level) => #(not= % rdd/disk-only))
  (fact "max and min work"
    (-> (rdd/parallelise [-1 2 3]) (rdd/max <)) => 3
    (-> (rdd/parallelise [-1 2 3]) (rdd/min >)) => 3)
  (fact "key-by works"
    (-> (rdd/parallelise ["a" "b" "c"])
        (rdd/key-by identity)
        rdd/collect) => [["a" "a"] ["b" "b"] ["c" "c"]])
  (fact "flat-map + filter works"
    (let [result-rdd (-> dummy-rdd
                         (rdd/flat-map aot/split-spaces)
                         (rdd/filter aot/equals-lewis))]
      (-> result-rdd rdd/collect count) => 18
      (-> result-rdd rdd/name) => (complement nil?)))
  (fact "map works"
    (-> dummy-rdd
        (rdd/map count)
        rdd/collect) => #(every? integer? %))
  (fact "reduce works"
    (-> dummy-rdd
        (rdd/map count)
        (rdd/reduce +)) => 2709
    (-> (rdd/parallelise [1 2 3 4 5])
        (rdd/reduce *)) => 120)
  (fact "map-to-pair + reduce-by-key + collect work"
    (-> dummy-pair-rdd
        (rdd/reduce-by-key +)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 18]
                         ["at no cost and with" 27]
                         ["of anyone anywhere" 27]
                         ["by Lewis Carroll" 18]
                         ["Project Gutenberg’s" 9]
                         ["This eBook is for the use" 27]]
    (-> dummy-pair-rdd
        rdd/collect) => #(and (every? vector? %)
                              (every? (comp (partial = 2) count) %)
                              (every? (comp string? first) %)
                              (every? (comp (partial = 1) second) %)))
  (fact "sort-by-key works"
    (-> dummy-pair-rdd
        (rdd/reduce-by-key +)
        rdd/sort-by-key
        rdd/collect) => #(= (sort %) %)
    (-> dummy-pair-rdd
        (rdd/reduce-by-key +)
        (rdd/sort-by-key false)
        rdd/collect) => #(= (sort %) (reverse %)))
  (fact "flat-map-to-pair works"
    (-> (rdd/parallelise ["hello world!"
                          "hello spark and geni!"
                          "the spark world is awesome!"])
        (rdd/flat-map-to-pair aot/split-spaces-and-pair)
        (rdd/reduce-by-key +)
        rdd/collect
        set) => #{["spark" 2] ["world" 1] ["and" 1] ["geni!" 1] ["the" 1]
                  ["awesome!" 1] ["is" 1] ["hello" 2] ["world!" 1]})
  (fact "map-partitions works"
    (-> (rdd/parallelise ["abc def" "ghi jkl" "mno pqr"])
        (rdd/map-partitions aot/map-split-spaces)
        rdd/collect) => ["abc" "def" "ghi" "jkl" "mno" "pqr"])
  (fact "map-partitions-to-pair works"
    (-> (rdd/parallelise ["abc def"])
        (rdd/map-partitions-to-pair aot/mapcat-split-spaces)
        rdd/collect) => [["abc" 1] ["def" 1]]
    (-> (rdd/parallelise ["abc def"])
        (rdd/map-partitions-to-pair aot/mapcat-split-spaces true)
        rdd/num-partitions) => (rdd/default-parallelism))
  (fact "map-partitions-with-index works"
    (-> (rdd/parallelise ["abc def" "ghi jkl" "mno pqr"])
        (rdd/map-partitions-with-index aot/map-split-spaces-with-index)
        rdd/collect) => #(and (every? integer? (map first %))
                              (= (set (map second %))
                                 #{"abc" "def" "ghi" "jkl" "mno" "pqr"})))
  (fact "zips work"
    (let [left (rdd/parallelise ["a b c" "d e f g h i"])
          right (rdd/parallelise ["j k l m n o" "pqr stu"])]
      (-> (rdd/zip left right)
          rdd/collect) => [["a b c" "j k l m n o"] ["d e f g h i" "pqr stu"]]
      (-> (rdd/zip-partitions left right aot/zip-split-spaces)
          rdd/collect) => ["aj" "bk" "cl" "dpqr" "estu"]
      (-> (rdd/zip-with-index left)
          rdd/collect) => [["a b c" 0] ["d e f g h i" 1]])
    (let [zipped-values (rdd/collect (rdd/zip-with-unique-id dummy-rdd))]
      (->> zipped-values (map second) set count) => (rdd/count dummy-rdd)))
  (fact "sample works"
    (let [rdd dummy-rdd]
      (rdd/count (rdd/sample rdd true 0.1)) => #(< 2 % 27)
      (rdd/count (rdd/sample rdd false 0.1 123)) => #(< 2 % 27)))
  (fact "coalesce works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (-> rdd (rdd/coalesce 1) rdd/collect) => ["abc" "def"]
      (-> rdd (rdd/coalesce 1 true) rdd/collect set) => #{"abc" "def"}))
  (fact "repartition works"
    (-> dummy-rdd (rdd/repartition 10) rdd/num-partitions) => 10)
  (fact "cartesian works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (rdd/collect (rdd/cartesian left right))
      => [["abc" "def"] ["abc" "ghi"] ["def" "def"] ["def" "ghi"]]))
  (fact "cache works"
    (-> dummy-rdd rdd/cache rdd/count) => 126)
  (fact "distinct works"
    (-> dummy-rdd rdd/distinct rdd/collect count) => 6
    (-> dummy-rdd (rdd/distinct 2) rdd/num-partitions) => 2
    (-> dummy-rdd (rdd/distinct 3) rdd/name) => #(string/includes? % "[3]"))
  (fact "zip-partitions works"
    (let [left (rdd/parallelise ["a b c" "d e f g h i"])
          right (rdd/parallelise ["j k l m n o" "pqr stu"])]
      (-> (rdd/zip-partitions left right aot/zip-split-spaces)
          rdd/collect)) => ["aj" "bk" "cl" "dpqr" "estu"])
  (fact "union works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (rdd/collect (rdd/union rdd rdd)) => ["abc" "def" "abc" "def"]))
  (fact "intersection works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (rdd/collect (rdd/intersection left right)) => ["def"]))
  (fact "glom works"
    (-> dummy-rdd rdd/glom rdd/count) => #(< % 126)))
