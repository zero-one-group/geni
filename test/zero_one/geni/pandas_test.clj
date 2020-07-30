(ns zero-one.geni.pandas-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(fact "On nlargest, nsmallest and nunique"
  (-> df-20
      (g/nlargest 3 :Price)
      (g/collect-col :Price)) => [1876000.0 1636000.0 1600000.0]
  (-> df-20
      (g/nsmallest 3 :Price)
      (g/collect-col :Price)) => [300000.0 441000.0 700000.0]
  (-> df-20
      (g/select :SellerG :Suburb)
      g/nunique
      g/first) => {(keyword "count(SellerG)") 6 (keyword "count(Suburb)")  1})

(fact "On shape"
  (g/shape df-20) => [20 21])

(fact "On value-counts"
  (-> df-20
      (g/select :SellerG :Suburb)
      g/value-counts
      (g/order-by (g/desc :count) :SellerG :Suburb)
      g/collect) => [{:SellerG "Biggin"  :Suburb "Abbotsford" :count 9}
                     {:SellerG "Jellis"  :Suburb "Abbotsford" :count 4}
                     {:SellerG "Nelson"  :Suburb "Abbotsford" :count 4}
                     {:SellerG "Collins" :Suburb "Abbotsford" :count 1}
                     {:SellerG "Greg"    :Suburb "Abbotsford" :count 1}
                     {:SellerG "LITTLE"  :Suburb "Abbotsford" :count 1}])

