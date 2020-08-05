(ns zero-one.geni.test-resources
  (:require
    [clojure.java.io :as io]
    [clojure.string :refer [split-lines split]]
    [zero-one.geni.core :as g]
    [zero-one.geni.defaults])
  (:import
    (java.io File)))

(def spark @zero-one.geni.defaults/spark)

(def melbourne-df
  (g/cache (g/read-parquet! spark "test/resources/melbourne_housing_snapshot.parquet")))

(def df-1 (g/limit melbourne-df 1))

(def df-20 (g/limit melbourne-df 20))

(def df-50 (g/limit melbourne-df 50))

(def libsvm-df
  (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt" {:num-features "780"}))

(def k-means-df
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt" {:num-features "780"}))

(def ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       split-lines
       (map #(split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (g/records->dataset spark)))

(defn create-temp-file! [extension]
  (let [temp-dir  (io/file (System/getProperty "java.io.tmpdir"))]
    (File/createTempFile "temporary" extension temp-dir)))
