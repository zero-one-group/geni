(ns zero-one.geni.column-test
  (:require
   [clojure.string]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :refer [melbourne-df df-1 df-20]]))

(fact "On explain"
  (interop/with-scala-out-str (g/explain (g/lead :Suburb 2) true))
  => "lead('Suburb, 2, null)\n")

(fact "On hash-code"
  (-> (df-1)
      (g/select (g/hash-code :Suburb))
      g/collect-vals
      ffirst) => int?)

(fact "On get-field and get-item"
  (-> (df-1)
      (g/with-column :x (g/struct {:s :SellerG :p :Price}))
      (g/with-column :xs [-1.0 -2.0])
      (g/select
       (g/get-field :x :s)
       (g/get-item :x :p)
       (g/get-item :xs (int 1)))
      g/collect-vals) => [["Biggin" 1480000.0 -2.0]])

(facts "On comparison and boolean functions" :slow
  (fact "null checks"
    (-> (df-1)
        (g/select
         (g/not-null? nil)
         (g/not-null? 1)
         (g/null? nil)
         (g/null? 1))
        g/collect-vals) => [[false true true false]])
  (fact "bitwise operations"
    (-> (df-1)
        (g/select
         (g/& 3 5)
         (g/| 3 5)
         (g/bitwise-xor 3 5))
        g/collect-vals) => [[1 7 6]])
  (fact "operations on maps"
    (-> (df-1)
        (g/select
         (g/&& {:a true  :b true})
         (g/&& {:a true  :b false})
         (g/|| {:a false :b false})
         (g/|| {:a true  :b false}))
        g/collect-vals) => [[true false false true]])
  (fact "zero-arity calls"
    (-> (df-1)
        (g/select
         (g/&&)
         (g/||))
        g/collect-vals) => [[true false]])
  (fact "rare comparison functions"
    (-> (df-1)
        (g/select
         (g/<=> 0 1)
         (g/<=> 1 1)
         (g/<=> 1 nil)
         (g/=== 1 nil)
         (g/=!= 1 1)
         (g/=!= 1 0))
        g/collect-vals) => [[false true false nil false true]])
  (fact "common comparison functions"
    (-> (df-1)
        (g/select
         (g/< 1)
         (g/< 1 2 3)
         (g/<= 1 1 1)
         (g/> 1 2 3)
         (g/>= 1 0.99 1.01)
         (g/&& true false)
         (g/|| true false))
        g/collect-vals) => [[true true true false false false true]])
  (fact "is in collection"
    (-> (df-1)
        (g/select
         (g/is-in-collection 1 [1 2])
         (g/is-in-collection 1 [2 3]))
        g/collect-vals) => [[true false]]))

(fact "On sorting functions" :slow
  (-> (df-20)
      (g/order-by (g/asc-nulls-first :BuildingArea))
      (g/collect-col :BuildingArea)
      first) => nil?
  (-> (df-20)
      (g/order-by (g/asc-nulls-last :BuildingArea))
      (g/collect-col :BuildingArea)
      last) => nil?
  (-> (df-20)
      (g/order-by (g/desc-nulls-first :BuildingArea))
      (g/collect-col :BuildingArea)
      first) => nil?
  (-> (df-20)
      (g/order-by (g/desc-nulls-last :BuildingArea))
      (g/collect-col :BuildingArea)
      last) => nil?)

(fact "On clojure idioms" :slow
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
      g/collect-vals) => [[2 0 true false true false false true]]
  (-> (df-1)
      (g/select
       {:a (g/short 1.0)
        :b (g/int 1.0)
        :c (g/long 1.0)
        :d (g/float 1)
        :e (g/double 1)
        :f (g/boolean 1)
        :g (g/byte 1)})
      g/dtypes) => {:a "ShortType"
                    :b "IntegerType"
                    :c "LongType"
                    :d "FloatType"
                    :e "DoubleType"
                    :f "BooleanType"
                    :g "ByteType"})

(facts "On string methods" :slow
  (fact "rlike should filter correctly"
    (let [includes-east-or-north? #(or (clojure.string/includes? % "East")
                                       (clojure.string/includes? % "North"))]
      (-> (melbourne-df)
          (g/filter (g/rlike :Suburb ".(East|North)"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-east-or-north? %)))
  (fact "like should filter correctly"
    (let [includes-south? #(clojure.string/includes? % "South")]
      (-> (melbourne-df)
          (g/filter (g/like :Suburb "%South%"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-south? %)))
  (fact "contains should filter correctly"
    (let [includes-west? #(clojure.string/includes? % "West")]
      (-> (melbourne-df)
          (g/filter (g/contains :Suburb "West"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-west? %)))
  (fact "starts-with should filter correctly"
    (-> (melbourne-df)
        (g/filter (g/starts-with :Suburb "East"))
        (g/select :Suburb)
        g/distinct
        (g/collect-col :Suburb)) => ["East Melbourne"])
  (fact "starts-with should filter correctly"
    (let [ends-with-west? #(= (last (clojure.string/split % #" ")) "West")]
      (-> (melbourne-df)
          (g/filter (g/ends-with :Suburb "West"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? ends-with-west? %))))

