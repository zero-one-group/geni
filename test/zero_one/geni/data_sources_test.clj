(ns zero-one.geni.data-sources-test
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.catalog :as c]
   [zero-one.geni.test-resources :refer [create-temp-file!
                                         melbourne-df
                                         libsvm-df
                                         spark
                                         with-fresh-session]])
  (:import
   (java.sql DriverManager)
   (org.apache.spark.sql AnalysisException)))

(def write-df
  (-> (melbourne-df) (g/select :Method :Type) (g/limit 5)))

(defn- sqlite-with-housing-table
  "The URL of a new SQLite database with an empty housing table. Spark 4 only
  treats a missing table as missing when the database's JDBC dialect says so,
  and Spark has no dialect for SQLite."
  []
  (let [url (str "jdbc:sqlite:" (create-temp-file! ".db"))]
    (Class/forName "org.sqlite.JDBC")
    (with-open [conn (DriverManager/getConnection url)
                stmt (.createStatement conn)]
      (.execute stmt "CREATE TABLE housing (Type TEXT)"))
    url))

(deftest ^:schema data-oriented-schema-test
  (let [dummy-df (-> (melbourne-df)
                     (g/limit 2)
                     g/->kebab-columns
                     (g/select {:rooms (g/struct :rooms :bathroom)
                                :coord (g/array :longtitude :lattitude)
                                ;; With ANSI mode, which Spark 4 turns on, a map of
                                ;; strings and doubles would be a map of doubles.
                                :prop  (g/map (g/lit "seller") :seller-g
                                              (g/lit "price") (g/cast :price "string"))}))
        temp-file (.toString (create-temp-file! "-complex.parquet"))]
    (g/write-parquet! dummy-df temp-file {:mode "overwrite"})
    (testing "correct dataframe baseline"
      (is (= {:coord "ArrayType(DoubleType,true)"
              :prop  "MapType(StringType,StringType,true)"
              :rooms (str "StructType("
                          "StructField(rooms,LongType,true),"
                          "StructField(bathroom,DoubleType,true))")}
             (g/dtypes dummy-df))))
    (testing "correct direct schema option"
      (is (= {:coord "ArrayType(LongType,true)"
              :prop  "MapType(StringType,StringType,true)"
              :rooms (str "StructType("
                          "StructField(rooms,IntegerType,true),"
                          "StructField(bathroom,FloatType,true))")}
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
                 g/dtypes))))
    (testing "correct data-oriented schema option"
      (is (= {:coord "ArrayType(ShortType,true)"
              :prop  "MapType(StringType,StringType,true)"
              :rooms (str "StructType("
                          "StructField(rooms,FloatType,true),"
                          "StructField(bathroom,LongType,true))")}
             (-> (g/read-parquet!
                  temp-file
                  {:schema {:coord [:short]
                            :prop  [:string :string]
                            :rooms {:rooms :float :bathroom :long}}})
                 g/dtypes))))))

(deftest ^:binary binary-data-test
  (let [binary-file "test/resources/geni.png"
        selected [:path :length :modificationTime :content]
        result (-> (g/read-binary! binary-file)
                   (g/select selected))]
    (testing "Read binary data"
      (is (= {:path "StringType",
              :length "LongType",
              :modificationTime "TimestampType",
              :content "BinaryType"}
             (-> result g/dtypes))))
    (testing "Read binary data - check for size"
      (is (= 52053
             (-> result
                 g/collect
                 first
                 :length))))))

(deftest ^:schema schema-option-test
  (let [csv-path "test/resources/sample_csv_data.csv"
        selected [:InvoiceDate :Price]]
    (testing "correct schemaless baseline"
      (is (= {:InvoiceDate "StringType" :Price "DoubleType"}
             (-> (g/read-csv! csv-path)
                 (g/select selected)
                 g/dtypes))))
    (testing "correct direct schema option"
      (is (= {:InvoiceDate "DateType" :Price "IntegerType"}
             (-> (g/read-csv! csv-path {:schema (g/struct-type
                                                 (g/struct-field :InvoiceDate :date true)
                                                 (g/struct-field :Price :int true))})
                 (g/select selected)
                 g/dtypes))))
    (testing "correct data-oriented schema option"
      (is (= {:InvoiceDate "DateType" :Price "LongType"}
             (-> (g/read-csv! csv-path {:schema {:InvoiceDate :date :Price :long}})
                 (g/select selected)
                 g/dtypes))))))

(deftest ^:excel excel-test
  (let [temp-file  (.toString (create-temp-file! ".xlsx"))
        read-df    (do
                     (g/write-xlsx! write-df temp-file {:mode "overwrite"})
                     (g/read-xlsx! temp-file))
        headerless (g/read-xlsx! temp-file {:header false :kebab-columns true})]
    (testing "read and write xlsx work"
      (is (= (g/collect write-df) (g/collect read-df)))
      (is (thrown? Exception (g/write-xlsx! write-df temp-file))))
    (testing "write-xlsx! writes to a new path"
      (let [new-file (str (.getParent (create-temp-file! ".xlsx")) "/new.xlsx")]
        (g/write-xlsx! write-df new-file)
        (is (= 5 (g/count (g/read-xlsx! new-file))))))
    (testing "read edge cases"
      (is (g/empty? (g/read-xlsx! temp-file {:sheet "Sheet2"})))
      (is (= 6 (g/count headerless)))
      (is (= {:c-0 "Method" :c-1 "Type"} (g/first headerless))))))

(deftest ^:edn edn-test
  (let [write-df  (-> (melbourne-df) (g/select :Price :Rooms) (g/limit 3))
        temp-file (.toString (create-temp-file! ".edn"))]
    (testing "write-edn! works as expected"
      (g/write-edn! write-df temp-file {:mode "overwrite"})
      (is (= [{:Price 1480000.0 :Rooms 2}
              {:Price 1035000.0 :Rooms 2}
              {:Price 1465000.0 :Rooms 3}]
             (edn/read-string (slurp temp-file))))
      (is (thrown? Exception (g/write-edn! write-df temp-file))))
    (testing "write-edn! writes to a new path"
      (let [new-file (str (.getParent (create-temp-file! ".edn")) "/new.edn")]
        (g/write-edn! write-df new-file)
        (is (= 3 (count (edn/read-string (slurp new-file)))))))
    (testing "read-edn! works as expected"
      (is (= [{:Price 1480000.0 :Rooms 2}
              {:Price 1035000.0 :Rooms 2}
              {:Price 1465000.0 :Rooms 3}]
             (g/collect (g/read-edn! temp-file))))
      (is (= ["price" "rooms"] (g/column-names (g/read-edn! temp-file {:kebab-columns true})))))))

(deftest ^:slow options-test
  (testing "infer-schema can be turned off"
    (is (= {:Price "StringType" :Rooms "StringType"}
           (let [write-df  (-> (melbourne-df) (g/select :Price :Rooms) (g/limit 5))
                 temp-file (.toString (create-temp-file! ".csv"))
                 read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                              (g/read-csv! temp-file {:infer-schema false}))]
             (g/dtypes read-df)))))
  (testing "kebab-columns option works"
    (is (= ["brebeuf-donnees-non-disponibles"
            "x-coordinate-state-plane"
            "col-with-underscore"
            "already-kebab-case"]
           (let [dataframe (g/table->dataset
                            [[1 2 3 4]]
                            ["Brébeuf (données non disponibles)"
                             "X Coordinate (State Plane)"
                             "col_with_underscore"
                             "already-kebab-case"])
                 temp-file (.toString (create-temp-file! ""))]
             (g/write-csv! dataframe temp-file {:mode "overwrite"})
             (g/column-names (g/read-csv! temp-file {:kebab-columns true})))))
    (is (= [:suburb
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
            :propertycount]
           (-> (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet" {:kebab-columns true})
               g/columns)))))

(deftest ^:slow writer-defaults-to-error-test
  (doall
   (for [write-fn! [g/write-avro!
                    g/write-csv!
                    g/write-json!
                    g/write-parquet!
                    g/write-text!]]
     (let [write-df  (g/select write-df :Method)
           temp-file (.toString (create-temp-file! ""))]
       (write-fn! write-df temp-file {:mode "overwrite"})
       (is (thrown? AnalysisException (write-fn! write-df temp-file))))))
  (let [temp-file (.toString (create-temp-file! ""))]
    (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
    (is (thrown? AnalysisException (g/write-libsvm! (libsvm-df) temp-file))))
  (let [write-df (g/select write-df :Type)
        options  {:driver  "org.sqlite.JDBC"
                  :url     (sqlite-with-housing-table)
                  :dbtable "housing"}]
    (g/write-jdbc! write-df (assoc options :mode "overwrite"))
    (is (thrown? AnalysisException (g/write-jdbc! write-df options)))))

(deftest ^:slow can-read-with-options-test
  (let [read-df (g/read-parquet!
                 "test/resources/melbourne_housing_snapshot.parquet"
                 {"mergeSchema" "true"})]
    (is (= 13580 (g/count read-df))))
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file {:header false}))]
    (is (not= (set (g/column-names read-df)) #{:Method :Type})))
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file {:num-features "780"}))]
    (is (= (g/collect (libsvm-df)) (g/collect read-df))))
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! temp-file {}))]
    (is (= (g/collect read-df) (g/collect write-df))))
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".txt"))
        read-df  (do (g/write-text! write-df temp-file {:mode "overwrite"})
                     (g/read-text! temp-file {}))]
    (is (= (g/collect-vals read-df) (g/collect-vals write-df)))))

(deftest can-read-and-write-csv-test
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode      "overwrite"
                                                       :delimiter "|"})
                     (g/read-csv! temp-file {:delimiter "|"}))]
    (is (= (g/collect read-df) (g/collect write-df))))
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file))]
    (is (= (g/collect read-df) (g/collect write-df))))
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file {:mode "overwrite"})
                     (g/read-csv! temp-file {}))]
    (is (= (g/column-names write-df) (g/column-names read-df)))))

(deftest can-read-and-write-avro-test
  (let [temp-file (.toString (create-temp-file! ".avro"))
        read-df  (do (g/write-avro! write-df temp-file {:mode "overwrite"})
                     (g/read-avro! temp-file))]
    (is (= (g/collect read-df) (g/collect write-df))))
  (let [temp-file (.toString (create-temp-file! ".avro"))
        read-df  (do (g/write-avro! write-df temp-file {:mode "overwrite"})
                     (g/read-avro! temp-file {}))]
    (is (= (g/collect read-df) (g/collect write-df)))))

(deftest can-read-and-write-parquet-test
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet! write-df temp-file {:mode "overwrite"})
                     (g/read-parquet! temp-file))]
    (is (= (g/collect read-df) (g/collect write-df)))))

(deftest can-read-and-write-libsvm-test
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file))]
    (is (= (map #(get-in % [:features :indices]) (g/collect read-df)) (map #(get-in % [:features :indices]) (g/collect (libsvm-df)))))
    (is (= (map #(get-in % [:features :values]) (g/collect read-df)) (map #(get-in % [:features :values]) (g/collect (libsvm-df)))))))

(deftest can-read-and-write-json-test
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file {:mode "overwrite"})
                     (g/read-json! temp-file))]
    (is (= (g/collect read-df) (g/collect write-df)))))

(deftest can-read-and-write-text-test
  (let [write-df  (g/select write-df :Type)
        temp-file (.toString (create-temp-file! ".text"))
        read-df   (do (g/write-text! write-df temp-file {:mode "overwrite"})
                      (g/read-text! temp-file))]
    (is (= (g/collect-vals read-df) (g/collect-vals write-df)))))

(deftest ^:slow can-read-and-write-jdbc-test
  (let [write-df (g/select write-df :Type)
        options  {:driver  "org.sqlite.JDBC"
                  :url     (sqlite-with-housing-table)
                  :dbtable "housing"}
        read-df  (do
                   (g/write-jdbc! write-df (assoc options :mode "overwrite"))
                   (g/read-jdbc! options))]
    (is (= (g/collect-vals read-df) (g/collect-vals write-df)))))

(deftest ^:slow can-write-parquet-with-partition-by-test
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet!
                      write-df
                      temp-file
                      {:mode "overwrite" :partition-by [:Method]})
                     (g/read-parquet! temp-file))]
    (is (= (set (g/collect read-df)) (set (g/collect write-df))))))

(deftest read-write-of-managed-tables-test
  (testing "throws if the table doesn't exist."
    (with-fresh-session
      (is (thrown? AnalysisException (g/read-table! @spark "i_dont_exist")))))

  (testing "can read and write tables"
    (with-fresh-session
      (let [dataset (g/range 3)
            table-name "tbl"]
        (g/write-table! dataset table-name)
        (is (c/table-exists? (c/catalog @spark) "tbl"))
        (is (= (g/collect (g/order-by (g/to-df dataset) :id)) (g/collect (g/order-by (g/read-table! table-name) :id))))))))
