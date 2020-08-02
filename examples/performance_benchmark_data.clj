(ns examples.performance-benchmark-data
  (:require
    [zero-one.geni.core :as g]))

(def transaction-ids
  ;; TODO: change to 1e7
  (g/table->dataset (mapv vector (range (int 1e5))) [:transaction-id]))

(def member-id-col
  ;; TODO: change to 1e6
  (g/int (g/* 1e4 (g/log (g// 1.0 (g/rand))))))

(def quantity-col
  (g/int (g/inc (g/log (g// 1.0 (g/rand))))))

(def unit-price-col
  (g/pow 2 (g/+ 15 (g/* 3 (g/rand)))))

(-> transaction-ids
    (g/with-column :member-id member-id-col)
    (g/with-column :quantity quantity-col)
    (g/with-column :unit-price unit-price-col)
    (g/describe :unit-price)
    g/show)
