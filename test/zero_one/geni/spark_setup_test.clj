(ns zero-one.geni.spark-setup-test
  (:require
    [clojure.string]
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.test-resources :refer [spark melbourne-df]])
  (:import
    (org.apache.spark.sql Dataset SparkSession)))

(fact "Test spark session and dataframe"
  spark => #(instance? SparkSession %)
  melbourne-df => #(instance? Dataset %)
  (-> spark .conf .getAll interop/scala-map->map)
  => #(= (% "spark.master") "local[*]"))

(fact "Test primary key is the product of address, date and seller"
  (-> melbourne-df
      (g/limit 100)
      (g/with-column
        "entry_id"
        (g/concat "Address" (g/lit "::") "Date" (g/lit "::") "SellerG"))
      (g/select "entry_id")
      g/distinct
      g/count) => 100)
