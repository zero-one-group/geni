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
    (testing "write-edn! keeps whole numbers that don't fit in a long"
      (let [big-file (.toString (create-temp-file! ".edn"))
            big-df   (g/sql @spark "SELECT CAST(123456789012345678901234567890 AS DECIMAL(38,0)) AS n")]
        (g/write-edn! big-df big-file {:mode "overwrite"})
        (is (= [{:n 123456789012345678901234567890N}] (edn/read-string (slurp big-file))))))
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
    (is (= ["_c0" "_c1"] (g/column-names read-df))))
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
    (is (= (g/column-names write-df) (g/column-names read-df))))
  (testing "without a header, when the options say so"
    (doseq [options [{:header false} {"header" "false"}]]
      (let [temp-file (.toString (create-temp-file! ".csv"))
            read-df   (do (g/write-csv! write-df temp-file (assoc options :mode "overwrite"))
                          (g/read-csv! temp-file {:header false}))]
        (is (= (g/collect-vals write-df) (g/collect-vals read-df)) (pr-str options))))))

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

;; LIBSVM's features are MLlib vectors, which a Spark Connect client can't read.
(deftest ^:classic can-read-and-write-libsvm-test
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file))]
    (is (= (map #(get-in % [:features :indices]) (g/collect read-df)) (map #(get-in % [:features :indices]) (g/collect (libsvm-df)))))
    (is (= (map #(get-in % [:features :values]) (g/collect read-df)) (map #(get-in % [:features :values]) (g/collect (libsvm-df))))))
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! (libsvm-df) temp-file {:mode "overwrite"})
                     (g/read-libsvm! temp-file {:num-features "780"}))]
    (is (= (g/collect (libsvm-df)) (g/collect read-df)))))

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

(deftest ^:slow read-jdbc-with-kebab-columns-test
  (let [url (str "jdbc:sqlite:" (create-temp-file! ".db"))]
    (with-open [conn (DriverManager/getConnection url)
                stmt (.createStatement conn)]
      (.execute stmt "CREATE TABLE tracks (TrackId INTEGER, UnitPrice REAL)")
      (.execute stmt "INSERT INTO tracks VALUES (1, 0.99)"))
    (is (= [{:track-id 1 :unit-price 0.99}]
           (g/collect (g/read-jdbc! {:driver        "org.sqlite.JDBC"
                                     :url           url
                                     :dbtable       "tracks"
                                     :kebab-columns true}))))))

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
      ;; Spark Connect only analyses a query when it needs its schema or rows.
      (is (thrown? AnalysisException (g/columns (g/read-table! @spark "i_dont_exist"))))))

  (testing "can read and write tables"
    (with-fresh-session
      (let [dataset (g/range 3)
            table-name "tbl"]
        (g/write-table! dataset table-name)
        (is (c/table-exists? (c/catalog @spark) "tbl"))
        (is (= (g/collect (g/order-by (g/to-df dataset) :id)) (g/collect (g/order-by (g/read-table! table-name) :id))))))))

(deftest generic-read-and-write-test
  (let [df   (g/records->dataset @spark [{:id 1 :v "a"} {:id 2 :v "b"}])
        dir  (.getParent (create-temp-file! ""))
        path #(str dir "/" %)]
    (testing "a format, a path, a mode and partitions"
      (g/write! df {:format "parquet" :path (path "p") :mode :overwrite :partition-by :v})
      (is (= [{:id 1 :v "a"} {:id 2 :v "b"}]
             (-> (g/read! {:format "parquet" :path (path "p")}) (g/order-by :id) g/collect))))
    (testing "several paths, a schema and reader options"
      (g/write! df {:format :json :path (path "j1")})
      (g/write! df {:format :json :path (path "j2")})
      (is (= {:id "IntegerType" :v "StringType"}
             (g/dtypes (g/read! @spark {:format :json
                                        :paths  [(path "j1") (path "j2")]
                                        :schema "id INT, v STRING"}))))
      (is (= 4 (g/count (g/read! {:format "json" :paths [(path "j1") (path "j2")]}))))
      (g/write! (g/records->dataset @spark [{:IdNum 1}]) {:format "csv" :path (path "c") :header true})
      (is (= [{:id-num 1}]
             (g/collect (g/read! {:format        "csv"
                                  :path          (path "c")
                                  :header        true
                                  :infer-schema  true
                                  :kebab-columns true}))))
      (testing "a string key goes as it is, and a keyword value as its name"
        (is (= {:IdNum "IntegerType"}
               (g/dtypes (g/read! {:format "csv" :path (path "c") "header" "true" "inferSchema" "true"}))))
        (is (= {:IdNum "StringType"}
               (g/dtypes (g/read! {:format "csv" :path (path "c") "header" "true" "infer-schema" "true"}))))
        (is (= 1 (g/count (g/read! {:format "csv" :path (path "c") :header true :mode :failfast}))))))
    (testing "a keyword :mode for the other writers"
      (g/write-parquet! df (path "k") {:mode :overwrite})
      (g/write-parquet! df (path "k") {:mode :overwrite})
      (g/write-edn! df (path "k.edn"))
      (g/write-edn! df (path "k.edn") {:mode :overwrite})
      (is (= 2 (g/count (g/read-parquet! (path "k")))))
      (is (= 2 (g/count (g/read-edn! (path "k.edn"))))))
    (testing "the errors"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #":path or :paths"
                            (g/read! {:path (path "p") :paths [(path "p")]})))
      (is (thrown-with-msg? AnalysisException #"bucketBy"
                            (g/write! df {:format "parquet" :path (path "b") :bucket-by [2 :id]}))))))

(deftest ^:slow generic-jdbc-test
  (let [options {:format  "jdbc"
                 :driver  "org.sqlite.JDBC"
                 :url     (sqlite-with-housing-table)
                 :dbtable "housing"}
        ;; Not write-df, which belongs to the session that the tests started
        ;; with, and with-fresh-session stops that one.
        df      (g/records->dataset @spark [{:Type "h"} {:Type "u"}])]
    (g/write! df (assoc options :mode :overwrite))
    (is (= ["h" "u"] (-> (g/read! options) (g/order-by :Type) (g/collect-col :Type))))))

(deftest parse-json-and-csv-test
  (let [json (g/records->dataset @spark [{:s "{\"a\": 1}"} {:s "{\"a\": 2, \"b\": \"x\"}"}])
        csv  (g/records->dataset @spark [{:s "1,x"} {:s "2,y"}])]
    (is (= [{:a 1 :b nil} {:a 2 :b "x"}] (-> json (g/parse-json :s) (g/order-by :a) g/collect)))
    (is (= {:a "IntegerType"} (-> json (g/parse-json :s {:schema {:a :int}}) g/dtypes)))
    (is (= [{:_c0 "1" :_c1 "x"} {:_c0 "2" :_c1 "y"}] (-> csv (g/parse-csv :s) (g/order-by :_c0) g/collect)))
    (is (= [{:n 1 :s "x"} {:n 2 :s "y"}]
           (-> csv (g/parse-csv :s {:schema "n INT, s STRING"}) (g/order-by :n) g/collect)))
    (is (= [{:id-num "1" :name "x"}]
           (-> (g/records->dataset @spark [{:s "IdNum,Name"} {:s "1,x"}])
               (g/parse-csv :s {:header true :kebab-columns true})
               g/collect)))))

(deftest table-writes-test
  (with-fresh-session
    (let [df          (g/records->dataset @spark [{:id 1 :v "a"} {:id 2 :v "b"}])
          described   (fn [table-name]
                        (->> (g/collect (g/sql @spark (str "DESCRIBE EXTENDED " table-name)))
                             (map (juxt :col_name :data_type))
                             (into {})))
          table-count #(g/count (g/read-table! %))]
      (testing "write-table! with buckets"
        (g/write-table! df "bucketed" {:format :parquet :bucket-by [2 :id] :sort-by :id})
        (is (= {"Provider" "parquet" "Num Buckets" "2" "Bucket Columns" "[`id`]" "Sort Columns" "[`id`]"}
               (select-keys (described "bucketed")
                            ["Provider" "Num Buckets" "Bucket Columns" "Sort Columns"]))))
      (testing "write-table! with buckets over several columns, and keyword names"
        (g/write-table! df :bucketed2 {:format :parquet :bucket-by [2 :id :v]})
        (is (= "[`id`, `v`]" (get (described "bucketed2") "Bucket Columns")))
        (is (= 2 (g/count (g/read-table! :bucketed2)))))
      (testing "read-table! with options"
        (is (= [:id :v] (g/columns (g/read-table! "bucketed" {:kebab-columns true}))))
        (is (= 2 (g/count (g/read-table! @spark "bucketed" {"mergeSchema" "false"})))))
      (testing "insert-into!"
        (g/insert-into! df "bucketed")
        (is (= 4 (table-count "bucketed")))
        (g/insert-into! df "bucketed" {:overwrite true})
        (is (= 2 (table-count "bucketed"))))
      (testing "write-to! creates a table"
        (g/write-to! df "created" {:mode             :create
                                   :using            "parquet"
                                   :partitioned-by   [:v]
                                   :table-properties {:geni.purpose "test" :version 3}})
        (is (= 2 (table-count "created")))
        (is (= #{"v=a" "v=b"} (set (g/collect-col (g/sql @spark "SHOW PARTITIONS created") :partition))))
        (is (= {"geni.purpose" "test" "version" "3"}
               (-> (->> (g/collect (g/sql @spark "SHOW TBLPROPERTIES created"))
                        (map (juxt :key :value))
                        (into {}))
                   (select-keys ["geni.purpose" "version"])))))
      (testing "write-to!'s other modes need a v2 table, which the session catalog doesn't have"
        (is (thrown-with-msg? AnalysisException #"v1 table" (g/write-to! df "created" {:mode :append})))
        (is (thrown? AnalysisException (g/write-to! df "created" {:mode :create-or-replace :using "parquet"}))))
      (testing "write-to!'s docstring condition, which picks the rows of one day"
        ;; The session catalog can't overwrite rows, so this filters by it.
        (let [doc       (:doc (meta #'g/write-to!))
              condition (binding [*ns* (the-ns 'zero-one.geni.data-sources-test)]
                          (eval (read-string (second (re-find #":condition (.*)\}\)" doc)))))]
          (is (= ["2026-10-01"]
                 (g/collect-col (g/filter (g/table->dataset @spark [["2026-10-01"] ["2026-10-02"]] [:day])
                                          condition)
                                :day)))))
      (testing "write-to!'s options"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"takes a :mode"
                              (g/write-to! df "created" {:mode :upsert})))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"takes a :condition"
                              (g/write-to! df "created" {:mode :overwrite})))))))
