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

(-> dataframe
    (g/filter (g/like :Suburb "%South%"))
    (g/select :Suburb)
    g/distinct
    (g/limit 5)
    g/show)

(-> dataframe
    (g/group-by :Suburb)
    (g/agg {:n (g/count "*")})
    (g/order-by (g/desc :n))
    (g/limit 5)
    g/show)

(-> dataframe
    (g/select :Suburb :Rooms :Price)
    g/print-schema)

(-> dataframe
    (g/describe :Price)
    g/show)

(letfn [(null-rate [col-name]
          (-> col-name
              g/null?
              g/double
              g/mean
              (g/as col-name)))]
  (-> dataframe
      (g/agg (map null-rate ["Car" "LandSize" "BuildingArea"]))
      g/collect))

