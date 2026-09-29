(ns zero-one.geni.spark-setup-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :refer [spark melbourne-df]])
  (:import
   (org.apache.spark.sql Dataset SparkSession)))

(deftest test-spark-session-and-dataframe-test
  (is (instance? SparkSession @spark))
  (is (instance? Dataset (melbourne-df)))
  (is (= ((-> @spark .conf .getAll interop/scala-map->map) "spark.master") "local[*]"))
  (is (clojure.string/includes? (-> @spark .sparkContext .getCheckpointDir .get) "target/checkpoint/"))
  (is (clojure.string/includes? (-> @spark .sparkContext .getConf g/to-debug-string) "spark.app.id"))
  (is (= {:spark.master                                  "local[*]",
          :spark.app.name                                "Geni App",
          :spark.sql.adaptive.enabled                    "true",
          :spark.sql.adaptive.coalescePartitions.enabled "true"}
         (select-keys (g/spark-conf @spark) [:spark.master
                                             :spark.app.name
                                             :spark.testing.memory
                                             :spark.sql.adaptive.enabled
                                             :spark.sql.adaptive.coalescePartitions.enabled]))))

(deftest test-primary-key-is-the-product-test
  (is (= 13580
         (-> (melbourne-df)
             (g/with-column
               "entry_id"
               (g/concat "Address" (g/lit "::") "Date" (g/lit "::") "SellerG"))
             (g/select "entry_id")
             g/distinct
             g/count))))
