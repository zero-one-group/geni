(ns zero-one.geni.test-resources
  (:require
    [zero-one.geni.core :as g]))

(defonce spark
  (g/create-spark-session {:configs {"spark.testing.memory" "2147480000"}}))

(defonce melbourne-df
  (g/cache
    (g/read-parquet! spark "test/resources/melbourne_housing_snapshot.parquet")))

(defonce libsvm-df
  (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(defonce k-means-df
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt"))
