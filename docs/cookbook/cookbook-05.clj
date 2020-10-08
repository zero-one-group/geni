(ns geni.cookbook-05
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(defn download-data! [source-url target-path]
  (if (-> target-path clojure.java.io/file .exists)
    :already-exists
    (do
      (clojure.java.io/make-parents target-path)
      (clojure.java.shell/sh "wget" "-O" target-path source-url)
      :downloaded)))

;; Part 5: String Operations

(def weather-2012
  (g/read-csv! "data/cookbook/weather-2012.csv"))

;; 5.1 Finding The Snowiest Months

(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/select :weather)
    g/distinct
    g/show)

(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/group-by :year :month)
    (g/agg {:n-days (g/count-distinct :day)})
    (g/order-by :year :month)
    g/show)

;; 5.2 Putting Snowiness and Temperature Together

(-> weather-2012
    (g/group-by :year :month)
    (g/agg
     {:n-snow-days (g/count-distinct
                    (g/when (g/like (g/lower :weather) "%snow%") :day))
      :n-days      (g/count-distinct :day)
      :mean-temp   (g/mean :temp-c)})
    (g/order-by :year :month)
    (g/select {:year      :year
               :month     :month
               :snowiness (g/format-number (g// :n-snow-days :n-days) 2)
               :mean-temp (g/format-number :mean-temp 1)})
    g/show)

;; 5.3 Finding Temperatures of Common Weather Descriptions

(-> weather-2012
    (g/with-column :weather-description (g/explode (g/split :weather ",")))
    (g/group-by :weather-description)
    (g/agg {:mean-temp (g/mean :temp-c)
            :n-days    (g/count-distinct :year :month :day)})
    (g/order-by (g/desc :mean-temp))
    (g/select {:weather-description :weather-description
               :mean-temp (g/format-number :mean-temp 1)
               :n-days :n-days})
    (g/show {:num-rows 25}))
