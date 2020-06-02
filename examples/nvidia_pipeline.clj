(ns examples.nvidia-pipeline
  (:require
    [zero-one.geni.core :as g]
    ;; [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

;; Load Data
(def dataframe
  (-> (g/read-parquet! spark "test/resources/housing.parquet")
      (g/with-column "rooms_per_house" (g// "total_rooms" "households"))
      (g/with-column "population_per_house" (g// "population" "households"))
      (g/with-column "bedrooms_per_house" (g// "total_bedrooms" "households"))
      (g/drop "total_rooms" "households" "population" "total_bedrooms")
      g/cache))

;; Summary Statistics
(-> dataframe
    (g/describe "median_income"
                "median_house_value"
                "bedrooms_per_house"
                "population_per_house")
    g/show)
; +-------+------------------+------------------+------------------+--------------------+
; |summary|median_income     |median_house_value|bedrooms_per_house|population_per_house|
; +-------+------------------+------------------+------------------+--------------------+
; |count  |5000              |5000              |4947              |5000                |
; |mean   |3.4760238599999966|177071.724        |1.119226894634528 |3.0392645333005506  |
; |stddev |1.8424610040929013|107669.60822163108|0.741916662913725 |8.484712111958718   |
; |min    |0.4999            |100000.0          |0.5               |1.0661764705882353  |
; |max    |9.8708            |99800.0           |34.06666666666667 |599.7142857142857   |
; +-------+------------------+------------------+------------------+--------------------+

(-> dataframe
    (g/select (g/corr "median_house_value" "median_income"))
    g/show)
; +---------------------------------------+
; |corr(median_house_value, median_income)|
; +---------------------------------------+
; |0.6747425007394737                     |
; +---------------------------------------+

(def dataframe-splits (g/random-split dataframe [0.8 0.2] 1234))
(def training-data (first dataframe-splits))
(def test-data (second dataframe-splits))

;; Feature Extraction and Pipelining
