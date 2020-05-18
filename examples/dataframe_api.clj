(ns examples.dataframe-api
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [melbourne-df]]))

(-> melbourne-df
    (g/filter (g/like "Suburb" "%South%"))
    (g/select "Suburb")
    g/distinct
    g/show)

(-> melbourne-df
    (g/group-by "Suburb")
    (g/agg (-> (g/count "*") (g/as "n")))
    (g/order-by (g/desc "n"))
    g/show)

