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
  => #(= (% "spark.master") "local[*]")
  (-> spark .sparkContext .getCheckpointDir .get)
  => #(clojure.string/includes? % "target/checkpoint/")
  (-> spark .sparkContext .getConf g/to-debug-string)
  => #(clojure.string/includes? % "spark.app.id")
  (select-keys (g/spark-conf spark) [:spark.master
                                     :spark.app.name
                                     :spark.testing.memory
                                     :spark.sql.adaptive.enabled
                                     :spark.sql.adaptive.coalescePartitions.enabled])
  => {:spark.master                                  "local[*]",
      :spark.app.name                                "Geni App",
      :spark.sql.adaptive.enabled                    "true",
      :spark.sql.adaptive.coalescePartitions.enabled "true",})


(fact "Test primary key is the product of address, date and seller"
  (-> melbourne-df
      (g/with-column
        "entry_id"
        (g/concat "Address" (g/lit "::") "Date" (g/lit "::") "SellerG"))
      (g/select "entry_id")
      g/distinct
      g/count) => 13580)
