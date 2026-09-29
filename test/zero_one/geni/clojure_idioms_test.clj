(ns zero-one.geni.clojure-idioms-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [spark df-20]]))

(deftest update-test
  (is (= [6 7 8 9 10]
         (-> (g/table->dataset @spark (mapv vector (range 5)) [:i])
             (g/update :i g/+ 1 2 3)
             (g/collect-col :i)))))

(deftest ^:slow cond-test
  (is (= #{"high" "medium" "low"}
         (-> (df-20)
             (g/with-column :cond (g/cond
                                    (g/< :Price 8e5) (g/lit "low")
                                    (g/< :Price 1e6) (g/lit "medium")
                                    :else            (g/lit "high")))
             (g/collect-col :cond)
             set))))

(deftest ^:slow condp-test
  (is (= [{:idx 1, :fb "1"}
          {:idx 2, :fb "2"}
          {:idx 3, :fb "fizz"}
          {:idx 4, :fb "4"}
          {:idx 5, :fb "buzz"}
          {:idx 6, :fb "fizz"}
          {:idx 7, :fb "7"}
          {:idx 8, :fb "8"}
          {:idx 9, :fb "fizz"}
          {:idx 10, :fb "buzz"}
          {:idx 11, :fb "11"}
          {:idx 12, :fb "fizz"}
          {:idx 13, :fb "13"}
          {:idx 14, :fb "14"}
          {:idx 15, :fb "fizzbuzz"}]
         (-> (g/table->dataset @spark (mapv vector (range 1 16)) [:idx])
             (g/with-column :fb (g/condp #(g/zero? (g/mod %2 %1)) :idx
                                  15 (g/lit "fizzbuzz")
                                  5 (g/lit "buzz")
                                  3 (g/lit "fizz")
                                  (g/str :idx)))
             g/collect)))
  (is (= [{:fb "fizz" :idx 3}
          {:fb "buzz" :idx 5}
          {:fb "fizz" :idx 6}
          {:fb "fizz" :idx 9}
          {:fb "buzz" :idx 10}
          {:fb "fizz" :idx 12}
          {:fb "fizzbuzz" :idx 15}]
         (-> (g/table->dataset @spark (mapv vector (range 1 16)) [:idx])
             (g/with-column :fb (g/condp #(g/zero? (g/mod %2 %1)) :idx
                                  15 (g/lit "fizzbuzz")
                                  5 (g/lit "buzz")
                                  3 (g/lit "fizz")))
             (g/remove (g/null? :fb))
             g/collect))))

(deftest ^:slow case-test
  (is (= #{{:SellerG "Biggin" :case 0}
           {:SellerG "Jellis" :case 2}
           {:SellerG "Nelson" :case 1}}
         (-> (df-20)
             (g/limit 10)
             (g/with-column :case (g/case :SellerG
                                    (g/lit "Biggin") 0
                                    (g/lit "Nelson") 1
                                    (g/lit "Jellis") 2))
             (g/select :SellerG :case)
             g/collect
             set)))
  (is (= #{{:SellerG "Biggin" :case 0}
           {:SellerG "Jellis" :case nil}
           {:SellerG "Nelson" :case 1}}
         (-> (df-20)
             (g/limit 10)
             (g/with-column :case (g/case :SellerG
                                    (g/lit "Biggin") 0
                                    (g/lit "Nelson") 1))
             (g/select :SellerG :case)
             g/collect
             set)))
  (is (= #{{:SellerG "Biggin" :case 123}
           {:SellerG "Jellis" :case 0}
           {:SellerG "Nelson" :case 1}}
         (-> (df-20)
             (g/limit 10)
             (g/with-column :case (g/case :SellerG
                                    (g/lit "Jellis") 0
                                    (g/lit "Nelson") 1
                                    123))
             (g/select :SellerG :case)
             g/collect
             set))))

(deftest ^:slow if-test
  (is (= #{"high" "low"}
         (-> (df-20)
             (g/with-column :if (g/if (g/< :Price 1e6)
                                  (g/lit "high")
                                  (g/lit "low")))
             (g/collect-col :if)
             set))))
