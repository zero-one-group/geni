(ns zero-one.geni.test-resources
  (:require
   [clojure.string :refer [split-lines split] :as string]
   [zero-one.geni.core :as g]
   [zero-one.geni.defaults]
   [clojure.java.io :as io])
  (:import
   (java.io File)
   (java.nio.file.attribute FileAttribute)
   (java.nio.file Files Paths)
   (java.util UUID)))

(def spark zero-one.geni.defaults/spark)

(defn melbourne-df []
  (g/read-parquet! @spark "test/resources/melbourne_housing_snapshot.parquet"))

(defn df-1 []
  (g/limit (melbourne-df) 1))

(defn df-20 []
  (g/limit (melbourne-df) 20))

(defn df-50 []
  (g/limit (melbourne-df) 50))

(defn libsvm-df []
  (g/read-libsvm! @spark "test/resources/sample_libsvm_data.txt" {:num-features "780"}))

(defn k-means-df []
  (g/read-libsvm! @spark "test/resources/sample_kmeans_data.txt" {:num-features "780"}))

(defn ratings-df []
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       split-lines
       (map #(split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (g/records->dataset @spark)))

(defn join-paths ^String
  [root & path-parts]
  (.toString (Paths/get root (into-array String path-parts))))

(def -tmp-dir-attr
  (into-array FileAttribute '()))

(defn create-temp-dir! ^File []
  (.toFile (Files/createTempDirectory "tmp-dir-" -tmp-dir-attr)))

(defn create-temp-file! ^File [extension]
  (let [temp-dir (create-temp-dir!)]
    (File/createTempFile "temporary" extension temp-dir)))

(defn recursive-delete-dir
  [^File file]
  (when (.isDirectory file)
    (doseq [file-in-dir (.listFiles file)]
      (recursive-delete-dir file-in-dir)))
  (io/delete-file file))

(defn delete-warehouse!
  []
  (let [wh-path (-> @spark .conf (.get "spark.sql.warehouse.dir") (string/replace "file:" ""))
        wh-dir (File. wh-path)]
    (when (.exists wh-dir)
      (recursive-delete-dir wh-dir))))

(def test-warehouses-root
  (join-paths (System/getProperty "user.dir") "spark-warehouses"))

(defn rand-wh-path
  []
  (str "file:" (join-paths test-warehouses-root (str (UUID/randomUUID)))))

(defn reset-session!
  []
  (.close @spark)
  (reset! spark (g/create-spark-session
                 (assoc-in zero-one.geni.defaults/session-config
                           [:configs :spark.sql.warehouse.dir]
                           (rand-wh-path)))))
