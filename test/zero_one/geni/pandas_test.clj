(ns zero-one.geni.pandas-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

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

