(ns zero-one.geni.data-sources-test
  (:require
    [midje.sweet :refer [fact => throws]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [create-temp-file!
                                          melbourne-df
                                          libsvm-df]])
  (:import
    (org.apache.spark.sql AnalysisException)))

(def write-df
  (-> melbourne-df (g/select :Method :Type) (g/limit 5)))

(fact "Writer defaults to error" :slow
  (doall
    (for [write-fn! [g/write-avro!
                     g/write-csv!
                     g/write-json!
                     g/write-parquet!
                     g/write-text!]]
      (let [write-df  (g/select write-df :Method)
            temp-file (.toString (create-temp-file! ""))]
        (write-fn! write-df temp-file {:mode "overwrite"})
        (write-fn! write-df temp-file) => (throws AnalysisException))))
  (let [temp-file (.toString (create-temp-file! ""))]
    (g/write-libsvm! libsvm-df temp-file {:mode "overwrite"})
    (g/write-libsvm! libsvm-df temp-file) => (throws AnalysisException))
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ""))
        options   {:driver  "org.sqlite.JDBC"
                   :url     (str "jdbc:sqlite:" temp-file)
                   :dbtable "housing"}]
    (g/write-jdbc! write-df (assoc options :mode "overwrite"))
    (g/write-jdbc! write-df options) => (throws AnalysisException)))

(fact "Can read with options" :slow
  (let [read-df (g/read-parquet!
                  "test/resources/melbourne_housing_snapshot.parquet"
                  {"mergeSchema" "true"})]
    (g/count read-df) => 13580)
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file {:header false}))]
    (set (g/column-names read-df)) => #(not= % #{:Method :Type}))
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! libsvm-df temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file {:num-features "780"}))]
    (g/collect read-df) => (g/collect libsvm-df))
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! temp-file {}))]
    (g/collect write-df) => (g/collect read-df))
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".txt"))
        read-df  (do (g/write-text! write-df temp-file {:mode "overwrite"})
                     (g/read-text! temp-file {}))]
    (g/collect-vals write-df) => (g/collect-vals read-df)))

(fact "Can read and write csv"
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode      "overwrite"
                                                       :delimiter "|"})
                     (g/read-csv! temp-file {:delimiter "|"}))]
    (g/collect write-df) => (g/collect read-df))
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file))]
    (g/collect write-df) => (g/collect read-df))
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file {}))]
    (g/column-names read-df) => (g/column-names write-df)))

(fact "Can read and write avro"
  (let [temp-file (.toString (create-temp-file! ".avro"))
        read-df  (do (g/write-avro! write-df temp-file {:mode "overwrite"})
                     (g/read-avro! temp-file))]
    (g/collect write-df) => (g/collect read-df))
  (let [temp-file (.toString (create-temp-file! ".avro"))
        read-df  (do (g/write-avro! write-df temp-file {:mode "overwrite"})
                     (g/read-avro! temp-file {}))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write parquet"
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet! write-df temp-file {:mode "overwrite"})
                     (g/read-parquet! temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write libsvm"
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! libsvm-df temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file))]
    (g/collect libsvm-df) => (g/collect read-df)))

(fact "Can read and write json"
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write text"
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".text"))
        read-df   (do (g/write-text! write-df temp-file {:mode "overwrite"})
                      (g/read-text! temp-file))]
    (g/collect-vals write-df) => (g/collect-vals read-df)))

(fact "Can read and write jdbc"
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".text"))
        read-df   (do
                    (g/write-jdbc!  write-df {:mode    "overwrite"
                                              :driver  "org.sqlite.JDBC"
                                              :url     (str "jdbc:sqlite:" temp-file)
                                              :dbtable "housing"})
                    (g/read-jdbc! {:driver  "org.sqlite.JDBC"
                                   :url     (str "jdbc:sqlite:" temp-file)
                                   :dbtable "housing"}))]
    (g/collect-vals write-df) => (g/collect-vals read-df)))
