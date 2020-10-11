(ns geni.cookbook-08
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

;; Part 8: Window Functions

(def product-revenue
  (g/table->dataset
   [["Thin"       "Cell phone" 6000]
    ["Normal"     "Tablet"     1500]
    ["Mini"       "Tablet"     5500]
    ["Ultra Thin" "Cell phone" 5000]
    ["Very Thin"  "Cell phone" 6000]
    ["Big"        "Tablet"     2500]
    ["Bendable"   "Cell phone" 3000]
    ["Foldable"   "Cell phone" 3000]
    ["Pro"        "Tablet"     4500]
    ["Pro2"       "Tablet"     6500]]
   [:product :category :revenue]))

(g/print-schema product-revenue)

;; 8.1 The Best and Second Best in Every Category

(def rank-by-category
  (g/windowed
   {:window-col   (g/dense-rank)
    :partition-by :category
    :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :rank-by-category rank-by-category)
    (g/filter (g/< :rank-by-category 3))
    g/show)

;; 8.2 Revenue Differences of Best and Second Best in Every Category
(def max-by-category
  (g/windowed
   {:window-col   (g/max :revenue)
    :partition-by :category}))

(-> product-revenue
    (g/with-column :max-by-category max-by-category)
    (g/with-column :revenue-diff (g/- :max-by-category :revenue))
    (g/order-by :category (g/desc :revenue))
    g/show)

;; 8.3 Revenue Differences to the Next Best in Every Category

(def next-best-by-category
  (g/windowed
   {:window-col   (g/lag :revenue 1)
    :partition-by :category
    :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :next-best-by-category next-best-by-category)
    (g/with-column :revenue-diff (g/- :next-best-by-category :revenue))
    g/show)

;; 8.4 Underperformance by One Sigma in Every Category

(def mean-by-category
  (g/windowed {:window-col (g/mean :revenue) :partition-by :category}))

(def std-by-category
  (g/windowed {:window-col (g/stddev :revenue) :partition-by :category}))

(-> product-revenue
    (g/with-column
      :z-stat-by-category
      (g// (g/- :revenue mean-by-category) std-by-category))
    (g/filter (g/< :z-stat-by-category -1))
    g/show)
