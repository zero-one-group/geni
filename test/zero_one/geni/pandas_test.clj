(ns zero-one.geni.pandas-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(fact "On quantile and median"
  (-> df-20
      (g/group-by :SellerG)
      (g/agg (g/quantile :Price [0.25 0.75]))
      g/dtypes) => {:SellerG "StringType"
                    (keyword "quantile(Price, (0.25, 0.75))")
                    "ArrayType(DoubleType,false)"}
  (-> df-20
      (g/select :SellerG :Price :Rooms)
      (g/group-by :SellerG)
      (g/quantile [0.25 0.75] [:Price :Rooms])
      g/column-names) => ["SellerG"
                          "quantile(Price, (0.25, 0.75))"
                          "quantile(Rooms, (0.25, 0.75))"]
  (-> df-20
      (g/group-by :SellerG)
      (g/agg (g/median :Price))
      g/collect) => [{:SellerG "Biggin"  (keyword "median(Price)") 1035000.0}
                     {:SellerG "Nelson"  (keyword "median(Price)") 1600000.0}
                     {:SellerG "Jellis"  (keyword "median(Price)") 941000.0}
                     {:SellerG "Greg"    (keyword "median(Price)") 441000.0}
                     {:SellerG "LITTLE"  (keyword "median(Price)") 1176500.0}
                     {:SellerG "Collins" (keyword "median(Price)") 955000.0}]
  (-> df-20
      (g/select :SellerG :Price :Rooms)
      (g/group-by :SellerG)
      (g/median :Price :Rooms)
      g/column-names) => ["SellerG" "median(Price)" "median(Rooms)"])

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

