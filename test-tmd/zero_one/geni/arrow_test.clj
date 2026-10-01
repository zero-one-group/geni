(ns ^:classic zero-one.geni.arrow-test
  "g/collect-to-arrow, read back with tech.ml.dataset."
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.arrow :as tmd-arrow]
            [zero-one.geni.core :as g]
            [zero-one.geni.arrow :as arrow]
            [zero-one.geni.test-resources
             :refer
             [k-means-df libsvm-df melbourne-df ratings-df]]))

(def temp-dir (System/getProperty "java.io.tmpdir"))

(deftest ^:arrow typed-action-test
  (testing "must not allow unknown type"
    (is (thrown? IllegalArgumentException (arrow/typed-action :get :unknown-type nil nil nil nil))))
  (testing "must not allow unknown action"
    (mapv
     (fn [col-type]
       (is (thrown? IllegalArgumentException (arrow/typed-action :unknown-action col-type nil nil nil nil))))
     [:string :double :float :long :integer :boolean :date])))

(deftest ^:arrow empty-dataframe-test
  (testing "writes arrow file with 0 rows and no schema"
    (is (= 0
           (-> (g/create-dataframe [] {:long    :long
                                       :int     :int
                                       :string  :string
                                       :float   :float
                                       :double  :double
                                       :date    :date
                                       :boolean :boolean})
               (g/collect-to-arrow 10 "/tmp")
               (first)
               (tmd-arrow/read-stream-dataset-copying)
               (ds/row-count))))))

(deftest ^:arrow melbourne-df-test

  (is (= 2
         (-> (melbourne-df)
             (g/select-columns [:Suburb])
             (g/collect-to-arrow 10000 temp-dir)
             count))))

(deftest size-of-collect-arrow-files-test
  (is (= 2
         (-> (melbourne-df)
             (g/collect-to-arrow 10000 temp-dir)
             count))))

(deftest tmd-can-read-it-all-test
  (let [arrow-files  (g/collect-to-arrow (melbourne-df) 20000 temp-dir)
        melbourne-ds (tmd-arrow/read-stream-dataset-copying (first arrow-files))]
    (is (= [21 13580] (ds/shape melbourne-ds)))
    (is (= (g/column-names (melbourne-df)) (ds/column-names melbourne-ds)))
    (is (= "85 Turner St" (str (first (get melbourne-ds "Address")))))
    (is (= 1480000.0 (first (get melbourne-ds "Price"))))))

(deftest split-in-rows-works-ok-test
  (let [arrow-files    (g/collect-to-arrow (melbourne-df) 10000 temp-dir)
        melbourne-ds-1 (tmd-arrow/read-stream-dataset-copying (first arrow-files))
        melbourne-ds-2 (tmd-arrow/read-stream-dataset-copying (second arrow-files))]
    (is (= [21 10000] (ds/shape melbourne-ds-1)))
    (is (= [21 3580] (ds/shape melbourne-ds-2)))))

(deftest ^:arrow crashes-and-failures-test
  (testing "does not crash"
    (g/collect-to-arrow (ratings-df) 10000 temp-dir)
    (-> (g/read-csv! "test/resources/boolean_data.csv")
        (g/collect-to-arrow 10 temp-dir))
    (-> (g/read-parquet! "test/resources/with_sql_date.parquet")
        (g/collect-to-arrow 10 temp-dir)))
  (testing "does fail"
    (is (thrown? IllegalArgumentException (-> (k-means-df)
                                              (g/collect-to-arrow 10 temp-dir))))
    (is (thrown? IllegalArgumentException (-> (libsvm-df)
                                              (g/collect-to-arrow 10000 temp-dir))))))

(deftest ^:arrow dates-test
  (testing "dates are corect"
    (let [with-date  (g/read-parquet! "test/resources/with_sql_date.parquet")
          ds
          (-> with-date
              (g/collect-to-arrow 10 temp-dir)
              first
              (tmd-arrow/read-stream-dataset-copying))]

      (is (= (.getTime (first (-> with-date (g/collect-col "date")))) (first (get ds "date")))))))

(deftest all-nil-data-frame-test
  (testing "all nils are written into the Arrow file"
    (let [dataset (-> (g/create-dataframe
                       [(g/row nil nil nil nil nil nil nil)]
                       {:long    :long
                        :int     :int
                        :string  :string
                        :float   :float
                        :double  :double
                        :date    :date
                        :boolean :boolean})
                      (g/collect-to-arrow 10 "/tmp")
                      (first)
                      (tmd-arrow/read-stream-dataset-copying))]
      (is (= 1 (ds/row-count dataset)))
      (is (= (repeat 7 [nil]) (map vec (vals dataset)))))))

(deftest empty-dataframe-2-test
  (testing "writes arrow file with 0 rows and no schema"
    (is (= 0
           (->
            (g/create-dataframe [] {:long    :long
                                    :int     :int
                                    :string  :string
                                    :float   :float
                                    :double  :double
                                    :date    :date
                                    :boolean :boolean})
            (g/collect-to-arrow 10 "/tmp")
            (first)
            (tmd-arrow/read-stream-dataset-copying)
            (ds/row-count))))))

