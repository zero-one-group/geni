(ns zero-one.geni.arrow-test
  (:require [midje.sweet :refer [=> fact facts throws]]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.arrow :as tmd-arrow]
            [zero-one.geni.core :as g]
            [zero-one.geni.arrow :as arrow]
            [zero-one.geni.test-resources
             :refer
             [k-means-df libsvm-df melbourne-df ratings-df]]))

(def temp-dir (System/getProperty "java.io.tmpdir"))

(facts "On typed-action" :arrow
  (fact "must not allow unknown type"
    (arrow/typed-action :unknown-type nil nil nil nil nil)
    => (throws IllegalArgumentException))
  (fact "must not allow unknown action"
    (mapv
     (fn [col-type]
       (arrow/typed-action col-type :unknown-action nil nil nil nil)
       => (throws IllegalArgumentException))
     [:string :double :float :long :integer :boolean :date])))

(facts "On empty dataframe" :arrow
  (fact "writes arrow file with 0 rows and no schema"
    (-> (g/create-dataframe [] (g/struct-type
                                (g/struct-field :long :long true)
                                (g/struct-field :int :int true)
                                (g/struct-field :string :string true)
                                (g/struct-field :float :float true)
                                (g/struct-field :double :double true)
                                (g/struct-field :date :date true)
                                (g/struct-field :boolean :boolean true)))
        (g/collect-to-arrow 10 "/tmp")
        (first)
        (tmd-arrow/read-stream-dataset-inplace)
        (ds/row-count))
    => 0))

(facts "On melbourne-df" :arrow
       (fact "On size of collect arrow files - string only")
    (-> (melbourne-df)
        (g/select-columns [:Suburb])
        (g/collect-to-arrow 10000 temp-dir)
        count) => 2)

  (fact "On size of collect arrow files"
    (-> (melbourne-df)
        (g/collect-to-arrow 10000 temp-dir)
        count) => 2)

  (fact "TMD can read it all"
    (let [arrow-files  (g/collect-to-arrow (melbourne-df) 20000 temp-dir)
          melbourne-ds (tmd-arrow/read-stream-dataset-inplace (first arrow-files))]
      (ds/shape melbourne-ds)  => [21 13580]
      (ds/column-names melbourne-ds) => (g/column-names (melbourne-df))
      (str (first (get melbourne-ds "Address"))) => "85 Turner St"
      (first (get melbourne-ds "Price")) => 1480000.0))

  (fact "split in rows works ok"
    (let [arrow-files    (g/collect-to-arrow (melbourne-df) 10000 temp-dir)
          melbourne-ds-1 (tmd-arrow/read-stream-dataset-copying (first arrow-files))
          melbourne-ds-2 (tmd-arrow/read-stream-dataset-copying (second arrow-files))]
      (ds/shape melbourne-ds-1) => [21 10000]
      (ds/shape melbourne-ds-2) => [21 3580]))

(facts "Crashes and failures" :arrow
  (fact "does not crash"
    (g/collect-to-arrow (ratings-df) 10000 temp-dir)
    (-> (g/read-csv! "test/resources/boolean_data.csv")
        (g/collect-to-arrow 10 temp-dir))
    (-> (g/read-parquet! "test/resources/with_sql_date.parquet")
        (g/collect-to-arrow 10 temp-dir)))
  (fact "does fail"
    (-> (k-means-df)
        (g/collect-to-arrow 10 temp-dir))
    => (throws IllegalArgumentException "No matching clause: :vector")
    (-> (libsvm-df)
        (g/collect-to-arrow 10000 temp-dir))
    => (throws IllegalArgumentException "No matching clause: :vector")))

(facts "On dates" :arrow
  (fact "dates are corect"
    (let [with-date  (g/read-parquet! "test/resources/with_sql_date.parquet")
          ds
          (-> with-date
              (g/collect-to-arrow 10 temp-dir)
              first
              (tmd-arrow/read-stream-dataset-copying))]

      (first (get ds "date")) =>
      (.getTime (first (-> with-date (g/collect-col "date")))))))

(facts "On all-nil data frame"
  (fact "all nils areet into arrow file"
    (-> (g/create-dataframe
         [(g/row nil nil nil nil nil nil nil)]
         (g/struct-type
          (g/struct-field :long :long true)
          (g/struct-field :int :int true)
          (g/struct-field :string :string true)
          (g/struct-field :float :float true)
          (g/struct-field :double :double true)
          (g/struct-field :date :date true)
          (g/struct-field :boolean :boolean true)))
        (g/collect-to-arrow 10 "/tmp")
        (first)
        (tmd-arrow/read-stream-dataset-copying)
        (ds/mapseq-reader)
        (first)
        vals) => [nil nil nil nil nil nil nil]))

(facts "On empty dataframe"
  (fact "writes arrow file with 0 rows and no schema"
    (->
     (g/create-dataframe [] (g/struct-type
                             (g/struct-field :long :long true)
                             (g/struct-field :int :int true)
                             (g/struct-field :string :string true)
                             (g/struct-field :float :float true)
                             (g/struct-field :double :double true)
                             (g/struct-field :date :date true)
                             (g/struct-field :boolean :boolean true)))
     (g/collect-to-arrow 10 "/tmp")
     (first)
     (tmd-arrow/read-stream-dataset-inplace)
     (ds/row-count)) => 0))

