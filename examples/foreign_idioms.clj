(ns examples.foreign-idioms
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [melbourne-df]]))

(def dataframe melbourne-df)

;; Pandas
(g/shape dataframe)

(-> dataframe
    (g/select :SellerG :Suburb)
    g/value-counts
    (g/limit 5)
    g/show)

(-> dataframe
    (g/select {:price-bins (g/cut :Price [8e5 1e6 1.2e6])})
    g/value-counts
    g/show)

(-> dataframe
    (g/select {:price :Price
               :bins  (g/qcut :Price [0.1 0.5 0.9])})
    (g/group-by :bins)
    (g/agg {:min   (g/min :price)
            :mean  (g/mean :price)
            :max   (g/max :price)
            :count (g/count "*")})
    g/show)

(-> dataframe
    (g/select {:price :Price
               :bins  (g/qcut :Price 7)})
    (g/group-by :bins)
    (g/agg {:min   (g/min :price)
            :mean  (g/mean :price)
            :max   (g/max :price)
            :count (g/count "*")})
    g/show)

;; NumPy
(-> dataframe
    (g/select {:land-size :LandSize
               :clipped   (g/clip :LandSize 100 200)})
    (g/limit 10)
    g/show)

;; tech.ml.dataset
(-> [{:a 1 :b 2} {:a 2 :c 3}]
    g/->dataset
    g/show)

(-> "test/resources/melbourne_housing_snapshot.parquet"
    (g/->dataset {:n-records 5 :column-whitelist [:Price
                                                  :Suburb
                                                  :LandSize]})
    g/show)


