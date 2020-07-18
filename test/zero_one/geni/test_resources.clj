(ns zero-one.geni.test-resources
  (:require
    [clojure.java.io :as io]
    [clojure.string :refer [split-lines split]]
    [zero-one.geni.core :as g]
    [zero-one.geni.dataset-creation :as dataset-creation])
  (:import
    (java.io File)))

(defonce spark
  (g/create-spark-session {:configs {:spark.testing.memory "3147480000"
                                     :spark.sql.adaptive.enabled "true"
                                     :spark.sql.adaptive.coalescePartitions.enabled "true"}
                           :checkpoint-dir "resources/checkpoint/"}))

(defonce melbourne-df
  (g/cache
    (g/read-parquet! spark "test/resources/melbourne_housing_snapshot.parquet")))
(defonce df-1 (g/cache (g/limit melbourne-df 1)))
(defonce df-20 (g/cache (g/limit melbourne-df 20)))
(defonce df-50 (g/cache (g/limit melbourne-df 50)))

(defonce libsvm-df
  (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt" {:num-features "780"}))

(defonce k-means-df
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt" {:num-features "780"}))

(defonce ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       split-lines
       (map #(split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (dataset-creation/records->dataset spark)))

(defn create-temp-file! [extension]
  (let [temp-dir  (io/file (System/getProperty "java.io.tmpdir"))]
    (File/createTempFile "temporary" extension temp-dir)))
