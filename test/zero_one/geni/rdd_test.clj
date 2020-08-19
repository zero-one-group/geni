(ns zero-one.geni.rdd-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.aot-functions :as aot]
    [zero-one.geni.defaults]
    [zero-one.geni.rdd :as rdd]))

(facts "On basic RDD operations" :rdd
  (-> (rdd/text-file "test/resources/rdd.txt")
      (rdd/map-to-pair aot/to-pair)
      rdd/group-by-key
      rdd/num-partitions) => #(< 1 %)
  (-> (rdd/text-file "test/resources/rdd.txt")
      (rdd/map-to-pair aot/to-pair)
      (rdd/group-by-key 7)
      rdd/num-partitions) => 7)

(facts "On basic RDD transformations + actions" :rdd
  (fact "foreach works"
    (-> (rdd/parallelise ["a" "b" "c"])
        (rdd/foreach identity)) => nil?)
  (fact "flat-map + filter works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/flat-map aot/split-spaces)
        (rdd/filter aot/equals-lewis)
        rdd/collect
        count) => 18)
  (fact "map works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map count)
        rdd/collect) => #(every? integer? %))
  (fact "reduce works"
    (-> (rdd/text-file "test/resources/rdd.txt" 1)
        (rdd/map count)
        (rdd/reduce +)) => 2709
    (-> (rdd/parallelise [1 2 3 4 5])
        (rdd/reduce *)) => 120)
  (fact "map-to-pair + reduce-by-key + collect work"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map-to-pair aot/to-pair)
        (rdd/reduce-by-key +)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 18]
                         ["at no cost and with" 27]
                         ["of anyone anywhere" 27]
                         ["by Lewis Carroll" 18]
                         ["Project Gutenberg’s" 9]
                         ["This eBook is for the use" 27]]
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map-to-pair aot/to-pair)
        rdd/collect) => #(and (every? vector? %)
                              (every? (comp (partial = 2) count) %)
                              (every? (comp string? first) %)
                              (every? (comp (partial = 1) second) %)))
  (fact "sort-by-key works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map-to-pair aot/to-pair)
        (rdd/reduce-by-key +)
        rdd/sort-by-key
        rdd/collect) => #(= (sort %) %)
    (-> (rdd/text-file "test/resources/rdd.txt")
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
  (fact "zip-partitions works"
    (let [left (rdd/parallelise ["a b c" "d e f g h i"])
          right (rdd/parallelise ["j k l m n o" "pqr stu"])]
      (-> (rdd/zip-partitions left right aot/zip-split-spaces)
          rdd/collect)) => ["aj" "bk" "cl" "dpqr" "estu"])
  (fact "sample works"
    (let [rdd (rdd/text-file "test/resources/rdd.txt")]
      (rdd/count (rdd/sample rdd true 0.1)) => #(< 5 % 20)
      (rdd/count (rdd/sample rdd false 0.1 123)) => #(< 5 % 20)))
  (fact "coalesce works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (-> rdd (rdd/coalesce 1) rdd/collect) => ["abc" "def"]
      (-> rdd (rdd/coalesce 1 true) rdd/collect set) => #{"abc" "def"}))
  (fact "repartition works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/repartition 10)
        rdd/num-partitions) => 10)
  (fact "cartesian works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (rdd/collect (rdd/cartesian left right))
      => [["abc" "def"] ["abc" "ghi"] ["def" "def"] ["def" "ghi"]])))

(facts "On basic RDD methods" :rdd
  (fact "cache works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        rdd/cache
        rdd/count) => 126)
  (fact "distinct works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        rdd/distinct
        rdd/collect
        count) => 6
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/distinct 2)
        rdd/num-partitions) => 2)
  (fact "union works"
    (let [rdd (rdd/parallelise ["abc" "def"])]
      (rdd/collect (rdd/union rdd rdd)) => ["abc" "def" "abc" "def"]))
  (fact "intersection works"
    (let [left (rdd/parallelise ["abc" "def"])
          right (rdd/parallelise ["def" "ghi"])]
      (rdd/collect (rdd/intersection left right)) => ["def"])))

