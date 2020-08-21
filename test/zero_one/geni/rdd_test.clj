(ns zero-one.geni.rdd-test
  (:require
    [clojure.java.io :as io]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.aot-functions :as aot]
    [zero-one.geni.defaults]
    [zero-one.geni.rdd :as rdd]
    [zero-one.geni.test-resources :refer [create-temp-file!]])
  (:import
    (org.apache.spark.api.java JavaSparkContext)))

(def dummy-rdd
  (rdd/text-file "test/resources/rdd.txt"))

(facts "On basic RDD saving and loading" :rdd
  (fact "save-as-text-file works"
    (let [write-rdd (rdd/parallelise (mapv (fn [_] (rand-int 100)) (range 100)))
          temp-file (.toString (create-temp-file! ".rdd"))
          read-rdd (do
                     (io/delete-file temp-file true)
                     (rdd/save-as-text-file write-rdd temp-file)
                     (rdd/text-file temp-file))]
      (rdd/count read-rdd) => (rdd/count write-rdd))))

(facts "On basic RDD fields" :rdd
  (let [rdd (rdd/parallelise [1])]
    (rdd/context rdd) => (partial instance? JavaSparkContext)
    (rdd/id rdd) => integer?
    (rdd/name rdd) => nil?
    (rdd/checkpointed? rdd) => false
    (rdd/empty? (rdd/parallelise [])) => true
    (rdd/empty? rdd) => false
    (rdd/empty? rdd) => false))
    ; (rdd/partitioner rdd) => nil?))

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
  (-> (rdd/text-file "test/resources/rdd.txt" 2)
      (rdd/map-to-pair aot/to-pair)
      rdd/group-by-key
      rdd/num-partitions) => #(< 1 %)
  (-> dummy-rdd
      (rdd/map-to-pair aot/to-pair)
      (rdd/group-by-key 7)
      rdd/num-partitions) => 7
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
    (-> dummy-rdd
        (rdd/flat-map aot/split-spaces)
        (rdd/filter aot/equals-lewis)
        rdd/collect
        count) => 18)
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
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        (rdd/reduce-by-key +)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 18]
                         ["at no cost and with" 27]
                         ["of anyone anywhere" 27]
                         ["by Lewis Carroll" 18]
                         ["Project Gutenberg’s" 9]
                         ["This eBook is for the use" 27]]
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        rdd/collect) => #(and (every? vector? %)
                              (every? (comp (partial = 2) count) %)
                              (every? (comp string? first) %)
                              (every? (comp (partial = 1) second) %)))
  (fact "sort-by-key works"
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
        (rdd/reduce-by-key +)
        rdd/sort-by-key
        rdd/collect) => #(= (sort %) %)
    (-> dummy-rdd
        (rdd/map-to-pair aot/to-pair)
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
        set)=> #{["spark" 2] ["world" 1] ["and" 1] ["geni!" 1] ["the" 1]
                 ["awesome!" 1] ["is" 1] ["hello" 2] ["world!" 1]})
  (fact "map-partitions works"
    (-> (rdd/parallelise ["abc def" "ghi jkl" "mno pqr"])
        (rdd/map-partitions aot/map-split-spaces)
        rdd/collect) => ["abc" "def" "ghi" "jkl" "mno" "pqr"])
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
      (rdd/count (rdd/sample rdd true 0.1)) => #(< 2 % 25)
      (rdd/count (rdd/sample rdd false 0.1 123)) => #(< 2 % 25)))
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
    (-> dummy-rdd (rdd/distinct 2) rdd/num-partitions) => 2)
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
