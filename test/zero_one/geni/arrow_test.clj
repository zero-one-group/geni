(ns zero-one.geni.arrow-test
  (:require [midje.sweet :refer [=> fact facts throws]]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.arrow :as tdm-arrow]
            [zero-one.geni.core :as g]
            [zero-one.geni.test-resources
             :refer
             [k-means-df libsvm-df melbourne-df ratings-df]]))

(def temp-dir (System/getProperty "java.io.tmpdir"))

(facts "On melbourne-df"
  (fact "On size of collect arrow files - string only"
    (->
     (melbourne-df)
     (g/select-columns [:Suburb])
     (g/collect-to-arrow 10000 temp-dir)
     count)

    => 2)

  (fact "On size of collect arrow files"
    (->
     (melbourne-df)
     (g/collect-to-arrow 10000 temp-dir)
     count)
    => 2)

  (fact "TMD can read it all"
    (let [arrow-files
          (-> (melbourne-df)
              (g/collect-to-arrow 20000 temp-dir))
          melbourne-ds (tdm-arrow/read-stream-dataset-inplace (first arrow-files))]
      (ds/shape melbourne-ds)  => [21 13580]
      (ds/column-names melbourne-ds) => (g/column-names (melbourne-df))
      (str (first  (get melbourne-ds "Address"))) => "85 Turner St"
      (first  (get melbourne-ds "Price")) => 1480000.0))

  (fact "split in rows works ok"
    (let [arrow-files
          (-> (melbourne-df)
              (g/collect-to-arrow 10000 temp-dir))
          melbourne-ds-1 (tdm-arrow/read-stream-dataset-copying (first arrow-files))
          melbourne-ds-2 (tdm-arrow/read-stream-dataset-copying (second arrow-files))]
      (ds/shape melbourne-ds-1)  => [21 10000]
      (ds/shape melbourne-ds-2)  => [21 3580])))

(facts "On ratings-df"
  (fact "does not crash"
    (-> (ratings-df)
        (g/collect-to-arrow 10000 temp-dir))))

(facts "On k-means-df"
  (fact "does fail"
    (-> (k-means-df)
        (g/collect-to-arrow 10 temp-dir))
    => (throws IllegalArgumentException "No matching clause: :vector")))

(facts "On libsvm-df"
  (fact "does fail"
    (-> (libsvm-df)
        (g/collect-to-arrow 10000 temp-dir))
    => (throws IllegalArgumentException "No matching clause: :vector")))

(facts "On boolean df"
  (fact "does not crash"
    (->
     (g/read-csv! "test/resources/boolean_data.csv")
     (g/collect-to-arrow 10 temp-dir))))

(facts "On sql-date df"
  (fact "does not crash"
    (->
     (g/read-parquet! "test/resources/with_sql_date.parquet")
     (g/collect-to-arrow 10 temp-dir)))
  (fact "dates are corect"
    (let [with-date  (g/read-parquet! "test/resources/with_sql_date.parquet")
          ds
          (-> with-date
              (g/collect-to-arrow 10 temp-dir)
              first
              (tdm-arrow/read-stream-dataset-copying))]

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
        (tdm-arrow/read-stream-dataset-copying)
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
     (tdm-arrow/read-stream-dataset-inplace)
     (ds/row-count)) => 0))
