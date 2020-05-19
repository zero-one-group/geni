(ns zero-one.geni.data-sources-test
  (:require
    [midje.sweet :refer [fact => throws]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [create-temp-file!
                                          melbourne-df
                                          libsvm-df
                                          spark]])
  (:import
    (org.apache.spark.sql AnalysisException)))

(def write-df
  (-> melbourne-df (g/select "Method" "Type") (g/limit 5)))

(fact "Writer defaults to error"
  (let [temp-file (.toString (create-temp-file! ".csv"))]
    (g/write-csv! write-df temp-file {:mode "overwrite"})
    (g/write-csv! write-df temp-file)) => (throws AnalysisException))

(fact "Can read with options"
  (let [read-df (g/read-parquet!
                  spark
                  "test/resources/melbourne_housing_snapshot.parquet"
                  {"mergeSchema" "true"})]
    (g/count read-df) => 13580)
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! spark temp-file {"header" "false"}))]
    (set (g/column-names read-df)) => #(not= % #{"Method" "Type"}))
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! libsvm-df temp-file {:mode "overwrite"})
                     (g/read-libsvm! spark temp-file {"numFeatures" "780"}))]
    (g/collect read-df) => (g/collect libsvm-df))
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! spark temp-file {}))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write csv"
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write parquet"
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet! write-df temp-file {:mode "overwrite"})
                     (g/read-parquet! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write libsvm"
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! libsvm-df temp-file {:mode "overwrite"})
                     (g/read-libsvm! spark temp-file))]
    (g/collect libsvm-df) => (g/collect read-df)))

(fact "Can read and write json"
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))
