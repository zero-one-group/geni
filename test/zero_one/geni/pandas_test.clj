(ns zero-one.geni.pandas-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [df-20]]))

(deftest replace-test
  (is (= #{"ABC" "Jellis" "Greg" "LITTLE" "Collins"}
         (-> (df-20)
             (g/select :SellerG)
             (g/with-column :SellerG (g/replace :SellerG ["Biggin" "Nelson"] "ABC"))
             (g/distinct)
             (g/collect-col :SellerG)
             (set))))
  (is (= #{"XYZ" "Jellis" "Greg" "DEF" "Collins"}
         (-> (df-20)
             (g/select :SellerG)
             (g/with-column :SellerG (g/replace :SellerG {"Biggin" "XYZ"
                                                          ["Nelson" "LITTLE"] "DEF"}))
             (g/distinct)
             (g/collect-col :SellerG)
             (set)))))

(deftest ^:slow cut-test
  (is (= #{"Price[-Infinity, 1000000.0]"
           "Price[1000000.0, Infinity]"}
         (-> (df-20)
             (g/with-column :cut (g/cut :Price [1e6]))
             (g/collect-col :cut)
             set)))
  (is (thrown? AssertionError (g/cut :Price [1.1e6 1e6]))))

(deftest ^:slow qcut-test
  (is (= #{"Price[0.0, 0.25]"
           "Price[0.25, 0.5]"
           "Price[0.5, 0.75]"
           "Price[0.75, 1.0]"}
         (-> (df-20)
             (g/with-column :qcut (g/qcut :Price 4))
             (g/collect-col :qcut)
             set)))
  (is (= #{"Price[0.0, 0.1]"
           "Price[0.1, 0.9]"
           "Price[0.9, 1.0]"}
         (-> (df-20)
             (g/with-column :qcut (g/qcut :Price [0.1 0.9]))
             (g/collect-col :qcut)
             set)))
  (is (thrown? AssertionError (g/qcut :Price [0.9 0.1])))
  (is (thrown? AssertionError (g/qcut :Price [0.8 1.2]))))

(deftest ^:slow interquartile-range-test
  (is (= [{:SellerG "Biggin" (keyword "iqr(Price)") 615000.0}
          {:SellerG "Nelson" (keyword "iqr(Price)") 0.0}]
         (-> (df-20)
             (g/limit 5)
             (g/group-by :SellerG)
             (g/agg (g/iqr :Price))
             g/collect)))
  (is (= {:SellerG "StringType"
          (keyword "iqr(Price)") "DoubleType"
          (keyword "iqr(Rooms)") "LongType"}
         (-> (df-20)
             (g/select :SellerG :Price :Rooms)
             (g/group-by :SellerG)
             (g/iqr :Price :Rooms)
             g/dtypes)))
  (is (= {:SellerG "StringType"
          (keyword "iqr(Price)") "DoubleType"
          (keyword "iqr(Rooms)") "LongType"}
         (-> (df-20)
             (g/select :SellerG :Price :Rooms)
             (g/group-by :SellerG)
             (g/iqr [:Price :Rooms])
             g/dtypes))))

(deftest ^:slow quantile-and-median-test
  (is (= {:SellerG "StringType"
          (keyword "quantile(Price, 0.25)") "DoubleType"}
         (-> (df-20)
             (g/group-by :SellerG)
             (g/agg (g/quantile :Price 0.25))
             g/dtypes)))
  (is (= {:SellerG "StringType"
          (keyword "quantile(Price, array(0.25, 0.75))")
          "ArrayType(DoubleType,false)"}
         (-> (df-20)
             (g/group-by :SellerG)
             (g/agg (g/quantile :Price [0.25 0.75]))
             g/dtypes)))
  (is (= ["SellerG"
          "quantile(Price, array(0.25, 0.75))"
          "quantile(Rooms, array(0.25, 0.75))"]
         (-> (df-20)
             (g/select :SellerG :Price :Rooms)
             (g/group-by :SellerG)
             (g/quantile [0.25 0.75] [:Price :Rooms])
             g/column-names)))
  (is (= [{:SellerG "Biggin"  (keyword "median(Price)") 1035000.0}
          {:SellerG "Nelson"  (keyword "median(Price)") 1600000.0}
          {:SellerG "Jellis"  (keyword "median(Price)") 941000.0}
          {:SellerG "Greg"    (keyword "median(Price)") 441000.0}
          {:SellerG "LITTLE"  (keyword "median(Price)") 1176500.0}
          {:SellerG "Collins" (keyword "median(Price)") 955000.0}]
         (-> (df-20)
             (g/group-by :SellerG)
             (g/agg (g/median :Price))
             g/collect)))
  (is (= ["SellerG" "median(Price)" "median(Rooms)"]
         (-> (df-20)
             (g/select :SellerG :Price :Rooms)
             (g/group-by :SellerG)
             (g/median :Price :Rooms)
             g/column-names))))

(deftest ^:slow nlargest-nsmallest-and-nunique-test
  (is (= [1876000.0 1636000.0 1600000.0]
         (-> (df-20)
             (g/nlargest 3 :Price)
             (g/collect-col :Price))))
  (is (= [300000.0 441000.0 700000.0]
         (-> (df-20)
             (g/nsmallest 3 :Price)
             (g/collect-col :Price))))
  (is (= {:SellerG 6 :Suburb 1}
         (-> (df-20)
             (g/select :SellerG :Suburb)
             g/nunique
             g/first))))

(deftest ^:slow value-counts-test
  (is (= [{:SellerG "Biggin"  :Suburb "Abbotsford" :count 9}
          {:SellerG "Jellis"  :Suburb "Abbotsford" :count 4}
          {:SellerG "Nelson"  :Suburb "Abbotsford" :count 4}
          {:SellerG "Collins" :Suburb "Abbotsford" :count 1}
          {:SellerG "Greg"    :Suburb "Abbotsford" :count 1}
          {:SellerG "LITTLE"  :Suburb "Abbotsford" :count 1}]
         (-> (df-20)
             (g/select :SellerG :Suburb)
             g/value-counts
             (g/order-by (g/desc :count) :SellerG :Suburb)
             g/collect))))

(deftest shape-test
  (is (= [20 21] (g/shape (df-20)))))
