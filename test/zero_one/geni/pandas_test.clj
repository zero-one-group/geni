(ns zero-one.geni.pandas-test
  (:require
    [midje.sweet :refer [throws fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(fact "On cut"
  (-> df-20
      (g/with-column :cut (g/cut :Price [1e6]))
      (g/collect-col :cut)
      set) => #{"Price[-Infinity, 1000000.0]"
                "Price[1000000.0, Infinity]"}
  (g/cut :Price [1.1e6 1e6]) => (throws AssertionError))

(fact "On qcut"
  (-> df-20
      (g/with-column :qcut (g/qcut :Price 4))
      (g/collect-col :qcut)
      set) => #{"Price[0.0, 0.25]"
                "Price[0.25, 0.5]"
                "Price[0.5, 0.75]"
                "Price[0.75, 1.0]"}
  (-> df-20
      (g/with-column :qcut (g/qcut :Price [0.1 0.9]))
      (g/collect-col :qcut)
      set) => #{"Price[0.0, 0.1]"
                "Price[0.1, 0.9]"
                "Price[0.9, 1.0]"}
  (g/qcut :Price [0.9 0.1]) => (throws AssertionError)
  (g/qcut :Price [0.8 1.2]) => (throws AssertionError))

(fact "On interquartile range" :slow
  (-> df-20
      (g/limit 5)
      (g/group-by :SellerG)
      (g/agg (g/iqr :Price))
      g/collect) => [{:SellerG "Biggin" (keyword "iqr(Price)") 615000.0}
                     {:SellerG "Nelson" (keyword "iqr(Price)") 0.0}]
  (-> df-20
      (g/select :SellerG :Price :Rooms)
      (g/group-by :SellerG)
      (g/iqr :Price :Rooms)
      g/dtypes) => {:SellerG "StringType"
                    (keyword "iqr(Price)") "DoubleType"
                    (keyword "iqr(Rooms)") "LongType"}
  (-> df-20
      (g/select :SellerG :Price :Rooms)
      (g/group-by :SellerG)
      (g/iqr [:Price :Rooms])
      g/dtypes) => {:SellerG "StringType"
                    (keyword "iqr(Price)") "DoubleType"
                    (keyword "iqr(Rooms)") "LongType"})

(fact "On quantile and median" :slow
  (-> df-20
      (g/group-by :SellerG)
      (g/agg (g/quantile :Price 0.25))
      g/dtypes) => {:SellerG "StringType"
                    (keyword "quantile(Price, 0.25)") "DoubleType"}
  (-> df-20
      (g/group-by :SellerG)
      (g/agg (g/quantile :Price [0.25 0.75]))
      g/dtypes) => {:SellerG "StringType"
                    (keyword "quantile(Price, array(0.25, 0.75))")
                    "ArrayType(DoubleType,false)"}
  (-> df-20
      (g/select :SellerG :Price :Rooms)
      (g/group-by :SellerG)
      (g/quantile [0.25 0.75] [:Price :Rooms])
      g/column-names) => ["SellerG"
                          "quantile(Price, array(0.25, 0.75))"
                          "quantile(Rooms, array(0.25, 0.75))"]
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

(fact "On nlargest, nsmallest and nunique" :slow
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

(fact "On value-counts" :slow
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

(fact "On shape"
  (g/shape df-20) => [20 21])
