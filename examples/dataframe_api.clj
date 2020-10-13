(ns examples.dataframe-api
  (:require
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [melbourne-df]]))

(def dataframe melbourne-df)

(-> dataframe
    (g/group-by :Suburb)
    g/count
    (g/order-by (g/desc :count))
    (g/limit 5)
    g/show)

;;=>
;; +--------------+-----+
;; |Suburb        |count|
;; +--------------+-----+
;; |Reservoir     |359  |
;; |Richmond      |260  |
;; |Bentleigh East|249  |
;; |Preston       |239  |
;; |Brunswick     |222  |
;; +--------------+-----+

(-> dataframe
    (g/filter (g/like :Suburb "%South%"))
    (g/select :Suburb)
    g/distinct
    (g/limit 5)
    g/show)

;;=>
;; +----------------+
;; |Suburb          |
;; +----------------+
;; |South Melbourne |
;; |South Kingsville|
;; |Clayton South   |
;; |Blackburn South |
;; |Vermont South   |
;; +----------------+

(-> dataframe
    (g/group-by :Suburb)
    (g/agg {:n (g/count "*")})
    (g/order-by (g/desc :n))
    (g/limit 5)
    g/show)

;;=>
;; +--------------+---+
;; |Suburb        |n  |
;; +--------------+---+
;; |Reservoir     |359|
;; |Richmond      |260|
;; |Bentleigh East|249|
;; |Preston       |239|
;; |Brunswick     |222|
;; +--------------+---+

(-> dataframe
    (g/select :Suburb :Rooms :Price)
    g/print-schema)

;;=>
;; root
;; |-- Suburb: string (nullable = true)
;; |-- Rooms: long (nullable = true)
;; |-- Price: double (nullable = true)

(-> dataframe
    (g/describe :Price)
    g/show)

;;=>
;; +-------+-----------------+
;; |summary|Price            |
;; +-------+-----------------+
;; |count  |13580            |
;; |mean   |1075684.079455081|
;; |stddev |639310.7242960163|
;; |min    |85000.0          |
;; |max    |9000000.0        |
;; +-------+-----------------+

(letfn [(null-rate [col-name]
          (-> col-name
              g/null?
              g/double
              g/mean
              (g/as col-name)))]
  (-> dataframe
      (g/agg (map null-rate ["Car" "LandSize" "BuildingArea"]))
      g/collect))

;;=>
#_({:Car 0.004565537555228277,
    :LandSize 0.0,
    :BuildingArea 0.47496318114874814})
