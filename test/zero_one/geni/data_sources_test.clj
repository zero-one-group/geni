(ns zero-one.geni.data-sources-test
  (:require
   [clojure.edn :as edn]
   [midje.sweet :refer [facts fact => throws with-state-changes before after]]
   [zero-one.geni.core :as g]
   [zero-one.geni.catalog :as c]
   [zero-one.geni.test-resources :refer [create-temp-file!
                                         create-temp-dir!
                                         join-paths
                                         melbourne-df
                                         libsvm-df
                                         spark
                                         reset-session!
                                         delete-warehouse!]])
  (:import
    (org.apache.spark.sql AnalysisException)
    (java.nio.file Paths Path FileSystem)))

(def write-df
  (-> (melbourne-df) (g/select :Method :Type) (g/limit 5)))

(facts "On data-oriented schema" :schema
  (let [dummy-df (-> (melbourne-df)
                     (g/limit 2)
                     g/->kebab-columns
                     (g/select {:rooms (g/struct :rooms :bathroom)
                                :coord (g/array :longtitude :lattitude)
                                :prop  (g/map (g/lit "seller") :seller-g
                                              (g/lit "price") :price)}))
        temp-file (.toString (create-temp-file! "-complex.parquet"))]
    (g/write-parquet! dummy-df temp-file {:mode "overwrite"})
    (fact "correct dataframe baseline"
      (g/dtypes dummy-df) => {:coord "ArrayType(DoubleType,true)"
                              :prop  "MapType(StringType,StringType,true)"
                              :rooms (str "StructType("
                                          "StructField(rooms,LongType,true), "
                                          "StructField(bathroom,DoubleType,true))")})
    (fact "correct direct schema option"
      (-> (g/read-parquet!
           temp-file
           {:schema (g/struct-type
                     (g/struct-field :rooms
                                     (g/struct-type
                                      (g/struct-field :rooms :int true)
                                      (g/struct-field :bathroom :float true))
                                     true)
                     (g/struct-field :coord (g/array-type :long true) true)
                     (g/struct-field :prop (g/map-type :string :string) true))})
          g/dtypes) => {:coord "ArrayType(LongType,true)"
                        :prop  "MapType(StringType,StringType,true)"
                        :rooms (str "StructType("
                                    "StructField(rooms,IntegerType,true), "
                                    "StructField(bathroom,FloatType,true))")})
    (fact "correct data-oriented schema option"
      (-> (g/read-parquet!
           temp-file
           {:schema {:coord [:short]
                     :prop  [:string :string]
                     :rooms {:rooms :float :bathroom :long}}})
          g/dtypes) => {:coord "ArrayType(ShortType,true)"
                        :prop  "MapType(StringType,StringType,true)"
                        :rooms (str "StructType("
                                    "StructField(rooms,FloatType,true), "
                                    "StructField(bathroom,LongType,true))")})))

(facts "On binary data" :binary
  (let [binary-file "test/resources/geni.png"
        selected [:path :length :modificationTime :content]
        result (-> (g/read-binary! binary-file)
                   (g/select selected))]
    (fact "Read binary data"
      (-> result g/dtypes) => {:path "StringType",
                               :length "LongType",
                               :modificationTime "TimestampType",
                               :content "BinaryType"})
    (fact "Read binary data - check for size"
      (-> result
          g/collect
          first
          :length) => 52053)))

(facts "On schema option" :schema
  (let [csv-path "test/resources/sample_csv_data.csv"
        selected [:InvoiceDate :Price]]
    (fact "correct schemaless baseline"
      (-> (g/read-csv! csv-path)
          (g/select selected)
          g/dtypes) => {:InvoiceDate "StringType" :Price "DoubleType"})
    (fact "correct direct schema option"
      (-> (g/read-csv! csv-path {:schema (g/struct-type
                                          (g/struct-field :InvoiceDate :date true)
                                          (g/struct-field :Price :int true))})
          (g/select selected)
          g/dtypes) => {:InvoiceDate "DateType" :Price "IntegerType"})
    (fact "correct data-oriented schema option"
      (-> (g/read-csv! csv-path {:schema {:InvoiceDate :date :Price :long}})
          (g/select selected)
          g/dtypes) => {:InvoiceDate "DateType" :Price "LongType"})))

(facts "On Excel" :excel
  (let [temp-file  (.toString (create-temp-file! ".xlsx"))
        read-df    (do
                     (g/write-xlsx! write-df temp-file {:mode "overwrite"})
                     (g/read-xlsx! temp-file))
        headerless (g/read-xlsx! temp-file {:header false :kebab-columns true})]
    (fact "read and write xlsx work"
      (g/collect read-df) => (g/collect write-df)
      (g/write-xlsx! write-df temp-file) => (throws Exception))
    (fact "read edge cases"
      (g/read-xlsx! temp-file {:sheet "Sheet2"}) => g/empty?
      (g/count headerless) => 6
      (g/first headerless) => {:c-0 "Method" :c-1 "Type"})))

(facts "On edn" :edn
  (let [write-df  (-> (melbourne-df) (g/select :Price :Rooms) (g/limit 3))
        temp-file (.toString (create-temp-file! ".edn"))]
    (fact "write-edn! works as expected"
      (g/write-edn! write-df temp-file {:mode "overwrite"})
      (edn/read-string (slurp temp-file)) => [{:Price 1480000.0 :Rooms 2}
                                              {:Price 1035000.0 :Rooms 2}
                                              {:Price 1465000.0 :Rooms 3}]
      (g/write-edn! write-df temp-file) => (throws Exception))
    (fact "read-edn! works as expected"
      (g/collect (g/read-edn! temp-file)) => [{:Price 1480000.0 :Rooms 2}
                                              {:Price 1035000.0 :Rooms 2}
                                              {:Price 1465000.0 :Rooms 3}]
      (g/column-names (g/read-edn! temp-file {:kebab-columns true}))
      => ["price" "rooms"])))

(facts "On options" :slow
  (fact "infer-schema can be turned off"
    (let [write-df  (-> (melbourne-df) (g/select :Price :Rooms) (g/limit 5))
          temp-file (.toString (create-temp-file! ".csv"))
          read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                       (g/read-csv! temp-file {:infer-schema false}))]
      (g/dtypes read-df)) => {:Price "StringType" :Rooms "StringType"})
  (fact "kebab-columns option works"
    (let [dataframe (g/table->dataset
                     [[1 2 3 4]]
                     ["Brébeuf (données non disponibles)"
                      "X Coordinate (State Plane)"
                      "col_with_underscore"
                      "already-kebab-case"])
          temp-file (.toString (create-temp-file! ""))]
      (g/write-csv! dataframe temp-file {:mode "overwrite"})
      (g/column-names (g/read-csv! temp-file {:kebab-columns true})))
    => ["brebeuf-donnees-non-disponibles"
        "x-coordinate-state-plane"
        "col-with-underscore"
        "already-kebab-case"]
    (-> (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet" {:kebab-columns true})
        g/columns) => [:suburb
                       :address
                       :rooms
                       :type
                       :price
                       :method
                       :seller-g
                       :date
                       :distance
                       :postcode
                       :bedroom-2
                       :bathroom
                       :car
                       :landsize
                       :building-area
                       :year-built
                       :council-area
                       :lattitude
                       :longtitude
                       :regionname
                       :propertycount]))

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
    (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
    (g/write-libsvm! (libsvm-df) temp-file) => (throws AnalysisException))
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
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file {:num-features "780"}))]
    (g/collect read-df) => (g/collect (libsvm-df)))
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
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file))]
    (map #(get-in % [:features :indices]) (g/collect (libsvm-df))) => (map #(get-in % [:features :indices]) (g/collect read-df))
    (map #(get-in % [:features :values]) (g/collect (libsvm-df))) => (map #(get-in % [:features :values]) (g/collect read-df))))

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

(fact "Can read and write jdbc" :slow
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".text"))
        read-df   (do
                    (g/write-jdbc! write-df {:mode    "overwrite"
                                             :driver  "org.sqlite.JDBC"
                                             :url     (str "jdbc:sqlite:" temp-file)
                                             :dbtable "housing"})
                    (g/read-jdbc! {:driver  "org.sqlite.JDBC"
                                   :url     (str "jdbc:sqlite:" temp-file)
                                   :dbtable "housing"}))]
    (g/collect-vals write-df) => (g/collect-vals read-df)))

(fact "Can write parquet with :partition-by option" :slow
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet!
                      write-df
                      temp-file
                      {:mode "overwrite" :partition-by [:Method]})
                     (g/read-parquet! temp-file))]
    (set (g/collect write-df)) => (set (g/collect read-df))))

(facts "On read/write of managed tables"
  (with-state-changes [(before :facts (reset-session!))
                       (after :facts (delete-warehouse!))]
    (fact "throws if the table doesn't exist."
      (g/read-table! @spark "i_dont_exist") => (throws AnalysisException))

    (fact "can read and write tables"
      (let [dataset (g/range 3)
            table-name "tbl"]
        (g/write-table! dataset table-name)
        (c/table-exists? (c/catalog @spark) "tbl") => true
        (g/collect (g/order-by (g/read-table! table-name) :id)) => (g/collect (g/order-by (g/to-df dataset) :id))))))

(fact "Can read and write Delta tables"
  (let [temp-dir (.toString (join-paths (.toString (create-temp-dir!)) "delta_test"))
        read-df (do (g/write-delta! write-df temp-dir {:mode "overwrite"})
                    (g/read-delta! temp-dir))]
      (g/collect write-df) => (g/collect read-df)))
