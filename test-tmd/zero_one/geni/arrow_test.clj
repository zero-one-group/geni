(ns ^:classic zero-one.geni.arrow-test
  "g/collect-to-arrow, read back with tech.ml.dataset."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.arrow :as tmd-arrow]
            [zero-one.geni.core :as g]
            [zero-one.geni.test-resources :as tr
             :refer
             [k-means-df libsvm-df melbourne-df ratings-df]])
  (:import
   (java.nio.file Files)))

(defn- collect-to-tmd
  "The result as collect-to-arrow writes it, read back as a dataset per file."
  [dataframe]
  (mapv tmd-arrow/read-stream-dataset-copying
        (g/collect-to-arrow dataframe (str (tr/create-temp-dir!)))))

(defn- collected-rows
  "The rows of the files that collect-to-arrow writes, read back with Arrow
  itself, since tech.ml.dataset can't read a struct, as an MLlib vector is."
  [dataframe]
  (let [read-rows (requiring-resolve 'zero-one.geni.arrow-rows/read-rows)]
    (mapcat #(read-rows (Files/readAllBytes (.toPath (io/file %))))
            (g/collect-to-arrow dataframe (str (tr/create-temp-dir!))))))

(def ^:private all-types
  {:long :long :int :int :string :string :float :float :double :double :date :date :boolean :boolean})

(deftest melbourne-df-test
  (testing "a file per Arrow batch, of at most 10,000 rows by default, which tech.ml.dataset reads"
    (let [[first-ds :as datasets] (collect-to-tmd (melbourne-df))]
      (is (= [[21 10000] [21 3580]] (map ds/shape datasets)))
      (is (= (g/column-names (melbourne-df)) (ds/column-names first-ds)))
      (is (= "85 Turner St" (str (first (get first-ds "Address")))))
      (is (= 1480000.0 (first (get first-ds "Price")))))))

(deftest other-types-test
  (testing "every type that Spark's Arrow batches take, MLlib vectors and dates too"
    (is (= (g/count (ratings-df)) (reduce + (map ds/row-count (collect-to-tmd (ratings-df))))))
    (is (= 6 (count (collected-rows (k-means-df)))))
    (is (= 100 (count (collected-rows (libsvm-df)))))
    (is (pos? (ds/row-count (first (collect-to-tmd (g/read-csv! "test/resources/boolean_data.csv"))))))
    (let [with-date (g/read-parquet! "test/resources/with_sql_date.parquet")]
      (is (= (str (first (g/collect-col with-date "date")))
             (str (first (get (first (collect-to-tmd with-date)) "date"))))))))

(deftest all-nil-data-frame-test
  (testing "all nils are written into the Arrow file"
    (let [dataset (apply ds/concat (collect-to-tmd (g/create-dataframe [(g/row nil nil nil nil nil nil nil)] all-types)))]
      (is (= 1 (ds/row-count dataset)))
      (is (= (repeat 7 [nil]) (map vec (vals dataset)))))))

(deftest nulls-after-the-first-row-test
  (testing "a null in a later row leaves the first row's value"
    (let [dataset (apply ds/concat (collect-to-tmd (g/create-dataframe [(g/row 1 "a") (g/row nil nil)]
                                                                       {:long :long :string :string})))]
      (is (= [1 nil] (vec (get dataset "long"))))
      (is (= ["a" nil] (map #(some-> % str) (get dataset "string")))))))
