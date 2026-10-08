(ns zero-one.geni.column-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :refer [spark melbourne-df df-1 df-20]]))

(deftest explain-test
  (is (clojure.string/starts-with? (interop/with-scala-out-str (g/explain (g/lead :Suburb 2) false))
                                   "lead(Suburb, 2, NULL"))
  (is (clojure.string/includes? (interop/with-scala-out-str (g/explain (g/lead :Suburb 2) true))
                                "lead")))

(deftest hash-code-test
  (is (int?
       (-> (df-1)
           (g/select (g/hash-code :Suburb))
           g/collect-vals
           ffirst))))

(deftest get-field-and-get-item-test
  (is (= [["Biggin" 1480000.0 -2.0]]
         (-> (df-1)
             (g/with-column :x (g/struct {:s :SellerG :p :Price}))
             (g/with-column :xs [-1.0 -2.0])
             (g/select
              (g/get-field :x :s)
              (g/get-item :x :p)
              (g/get-item :xs (int 1)))
             g/collect-vals))))

(deftest comparison-and-boolean-functions-test
  (testing "null checks"
    (is (= [[false true true false]]
           (-> (df-1)
               (g/select
                (g/not-null? nil)
                (g/not-null? 1)
                (g/null? nil)
                (g/null? 1))
               g/collect-vals))))
  (testing "bitwise operations"
    (is (= [[1 7 6]]
           (-> (df-1)
               (g/select
                (g/& 3 5)
                (g/| 3 5)
                (g/bitwise-xor 3 5))
               g/collect-vals))))
  (testing "operations on maps"
    (is (= [[true false false true]]
           (-> (df-1)
               (g/select
                (g/&& {:a true  :b true})
                (g/&& {:a true  :b false})
                (g/|| {:a false :b false})
                (g/|| {:a true  :b false}))
               g/collect-vals))))
  (testing "zero-arity calls"
    (is (= [[true false]]
           (-> (df-1)
               (g/select
                (g/&&)
                (g/||))
               g/collect-vals))))
  (testing "rare comparison functions"
    (is (= [[false true false nil false true]]
           (-> (df-1)
               (g/select
                (g/<=> 0 1)
                (g/<=> 1 1)
                (g/<=> 1 nil)
                (g/=== 1 nil)
                (g/=!= 1 1)
                (g/=!= 1 0))
               g/collect-vals))))
  (testing "common comparison functions"
    (is (= [[true true true false false false true]]
           (-> (df-1)
               (g/select
                (g/< 1)
                (g/< 1 2 3)
                (g/<= 1 1 1)
                (g/> 1 2 3)
                (g/>= 1 0.99 1.01)
                (g/&& true false)
                (g/|| true false))
               g/collect-vals))))
  (testing "is in collection"
    (is (= [[true false]]
           (-> (df-1)
               (g/select
                (g/is-in-collection 1 [1 2])
                (g/is-in-collection 1 [2 3]))
               g/collect-vals)))))

(deftest sorting-functions-test
  (is (nil?
       (-> (df-20)
           (g/order-by (g/asc-nulls-first :BuildingArea))
           (g/collect-col :BuildingArea)
           first)))
  (is (nil?
       (-> (df-20)
           (g/order-by (g/asc-nulls-last :BuildingArea))
           (g/collect-col :BuildingArea)
           last)))
  (is (nil?
       (-> (df-20)
           (g/order-by (g/desc-nulls-first :BuildingArea))
           (g/collect-col :BuildingArea)
           first)))
  (is (nil?
       (-> (df-20)
           (g/order-by (g/desc-nulls-last :BuildingArea))
           (g/collect-col :BuildingArea)
           last))))

(deftest clojure-idioms-test
  (is (= [[2 0 true false true false false true]]
         (-> (df-1)
             (g/select
              (g/inc 1)
              (g/dec 1)
              (g/= 1 1)
              (g/zero? 1)
              (g/pos? 1)
              (g/neg? 1)
              (g/even? 1)
              (g/odd? 1))
             g/collect-vals)))
  (is (= {:a "ShortType"
          :b "IntegerType"
          :c "LongType"
          :d "FloatType"
          :e "DoubleType"
          :f "BooleanType"
          :g "ByteType"}
         (-> (df-1)
             (g/select
              {:a (g/short 1.0)
               :b (g/int 1.0)
               :c (g/long 1.0)
               :d (g/float 1)
               :e (g/double 1)
               :f (g/boolean 1)
               :g (g/byte 1)})
             g/dtypes))))

(deftest string-methods-test
  (testing "rlike should filter correctly"
    (let [includes-east-or-north? #(or (clojure.string/includes? % "East")
                                       (clojure.string/includes? % "North"))]
      (is (every? includes-east-or-north? (-> (melbourne-df)
                                              (g/filter (g/rlike :Suburb ".(East|North)"))
                                              (g/select :Suburb)
                                              g/distinct
                                              (g/collect-col :Suburb))))))
  (testing "like should filter correctly"
    (let [includes-south? #(clojure.string/includes? % "South")]
      (is (every? includes-south? (-> (melbourne-df)
                                      (g/filter (g/like :Suburb "%South%"))
                                      (g/select :Suburb)
                                      g/distinct
                                      (g/collect-col :Suburb))))))
  (testing "contains should filter correctly"
    (let [includes-west? #(clojure.string/includes? % "West")]
      (is (every? includes-west? (-> (melbourne-df)
                                     (g/filter (g/contains :Suburb "West"))
                                     (g/select :Suburb)
                                     g/distinct
                                     (g/collect-col :Suburb))))))
  (testing "starts-with should filter correctly"
    (is (= ["East Melbourne"]
           (-> (melbourne-df)
               (g/filter (g/starts-with :Suburb "East"))
               (g/select :Suburb)
               g/distinct
               (g/collect-col :Suburb)))))
  (testing "starts-with should filter correctly"
    (let [ends-with-west? #(= (last (clojure.string/split % #" ")) "West")]
      (is (every? ends-with-west? (-> (melbourne-df)
                                      (g/filter (g/ends-with :Suburb "West"))
                                      (g/select :Suburb)
                                      g/distinct
                                      (g/collect-col :Suburb)))))))

(deftest ilike-test
  (is (= ["Abc" "aBz"]
         (-> (g/records->dataset @spark [{:s "Abc"} {:s "xyz"} {:s "aBz"}])
             (g/filter (g/ilike :s "a%"))
             (g/order-by :s)
             (g/collect-col :s)))))

(deftest with-field-and-drop-fields-test
  (let [structs (g/sql @spark "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS s")
        s-col   #(-> structs (g/select {:s %}) (g/collect-col :s))]
    (is (= [{:a 1 :b 2 :c 3 :d 4}] (s-col (g/with-field :s :d 4))))
    (is (= [{:a 10 :b 2 :c 3}] (s-col (g/with-field :s :a 10))))
    (is (= [{:a 1}] (s-col (g/drop-fields :s :b :c))))))

(deftest not-equal-test
  (is (= [0 2] (-> (g/range 3)
                   (g/filter (g/=!= :id 1))
                   (g/order-by :id)
                   (g/collect-col :id)))))
