(ns examples.dataframe-api
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [melbourne-df]]))

(-> melbourne-df
    (g/filter (g/like "Suburb" "%South%"))
    (g/select "Suburb")
    g/distinct
    (g/limit 5)
    g/show)

(-> melbourne-df
    (g/group-by "Suburb")
    (g/agg (-> (g/count "*") (g/as "n")))
    (g/order-by (g/desc "n"))
    (g/limit 5)
    g/show)

(-> melbourne-df
    (g/select "Suburb" "Rooms" "Price")
    g/print-schema)

(-> melbourne-df
    (g/describe "Price")
    g/show)

(letfn [(null-rate [col-name]
          (-> col-name
              g/null?
              (g/cast "double")
              g/mean
              (g/as col-name)))]
  (-> melbourne-df
      (g/agg (map null-rate ["Car" "LandSize" "BuildingArea"]))
      g/collect))

