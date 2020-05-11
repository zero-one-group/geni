(ns zero-one.geni.test-resources
  (:require
    [clojure.string :refer [split-lines split]]
    [zero-one.geni.core :as g]
    [zero-one.geni.dataset :as ds]))

(defonce spark
  (g/create-spark-session {:configs {"spark.testing.memory" "2147480000"}}))

(defonce melbourne-df
  (g/cache
    (g/read-parquet! spark "test/resources/melbourne_housing_snapshot.parquet")))

(defonce libsvm-df
  (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(defonce k-means-df
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt"))

(defonce ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       split-lines
       (map #(split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (ds/records->dataset spark)))
