(ns zero-one.geni.dataset-creation-test
  (:require
   [clojure.string :refer [includes?]]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :as tr])
  (:import
   (java.time Duration Instant LocalDate LocalDateTime Period)
   (java.util UUID)
   (org.apache.spark.sql Dataset
                         Row)
   (org.apache.spark.sql.types StructField
                               StructType)))

(deftest ^:empty-dataset creation-of-empty-dataset-test
  (testing "correct creation"
    (is (g/empty? (g/create-dataframe [] {})))
    (is (g/empty? (g/table->dataset @tr/spark [] [])))
    (is (g/empty? (g/map->dataset @tr/spark {})))
    (is (g/empty? (g/records->dataset @tr/spark {}))))
  (testing "correct schema"
    (is (= {:i "IntegerType"} (g/dtypes (g/create-dataframe @tr/spark [] {:i :int}))))
    (is (= {:j "FloatType"}
           (g/dtypes
            (g/create-dataframe @tr/spark [] (g/struct-type (g/struct-field :j :float true))))))))

(deftest ^:schema can-instantiate-dataframe-with-data-oriented-test
  (testing "of simple data type fields"
    (is (= {:number "IntegerType"
            :word "StringType"}
           (g/dtypes
            (g/create-dataframe
             @tr/spark
             [(g/row (int 32) "horse")
              (g/row (int 64) "mouse")]
             {:number :int :word :str})))))
  (testing "of struct fields"
    (is (= {:coord "StructType(StructField(x,IntegerType,true),StructField(y,IntegerType,true))"}
           (g/dtypes
            (g/create-dataframe
             @tr/spark
             [(g/row (g/row (int 27) (int 42)))
              (g/row (g/row (int 57) (int 18)))]
             {:coord {:x :int :y :int}})))))
  (testing "of struct array fields"
    (is (= {:coords "ArrayType(StructType(StructField(x,IntegerType,true),StructField(y,IntegerType,true)),true)"}
           (g/dtypes
            (g/create-dataframe
             @tr/spark
             [(g/row [(g/row (int 27) (int 42))])
              (g/row [(g/row (int 57) (int 18))])]
             {:coords [{:x :int :y :int}]}))))))

(deftest building-blocks-test
  (testing "can instantiate rows"
    (is (instance? Row (g/row [2]))))
  (testing "can instantiate struct field and type"
    (let [field (g/struct-field :number :integer true)]
      (is (instance? StructField field))
      (is (instance? StructType (g/struct-type field)))))
  (testing "can instantiate example dataframes"
    (let [expected-dtypes {:number "LongType" :word "StringType"}]
      (is (= expected-dtypes
             (g/dtypes
              (g/to-df
               [[8 "bat"] [64 "mouse"] [-27 "horse"]]
               [:number :word]))))
      (is (= expected-dtypes
             (g/dtypes
              (g/create-dataframe
               @tr/spark
               [(g/row 8 "bat")
                (g/row 64 "mouse")
                (g/row -27 "horse")]
               (g/struct-type
                (g/struct-field :number :long true)
                (g/struct-field :word :string true))))))
      (is (= expected-dtypes
             (g/dtypes
              (g/table->dataset
               [[8 "bat"] [64 "mouse"] [-27 "horse"]]
               [:number :word]))))
      (is (= expected-dtypes
             (g/dtypes
              (g/map->dataset
               @tr/spark
               {:number [8 64 -27]
                :word   ["bat" "mouse" "horse"]}))))
      (is (= expected-dtypes
             (g/dtypes
              (g/records->dataset
               @tr/spark
               [{:number   8 :word "bat"}
                {:number  64 :word "mouse"}
                {:number -27 :word "horse"}])))))))

(deftest map-dataset-test
  (testing "should create the right dataset"
    (let [dataset (g/map->dataset
                   @tr/spark
                   {:a [1 4]
                    :b [2.0 5.0]
                    :c ["a" "b"]})]
      (is (instance? Dataset dataset))
      (is (= ["a" "b" "c"] (g/column-names dataset)))
      (is (= [[1 2.0 "a"] [4 5.0 "b"]] (g/collect-vals dataset)))))
  (testing "should create the right schema even with nils"
    (let [dataset (g/map->dataset
                   @tr/spark
                   {:a [nil 4]
                    :b [2.0 5.0]})]
      (is (= [[nil 2.0] [4 5.0]] (g/collect-vals dataset)))))
  (testing "should create the right null column"
    (let [dataset (g/map->dataset
                   @tr/spark
                   {:a [1 4]
                    :b [nil nil]})]
      (is (= [[1 nil] [4 nil]] (g/collect-vals dataset))))))

(deftest records-dataset-test
  (testing "should create the right dataset"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:a 1 :b 2.0 :c "a"}
                    {:a 4 :b 5.0 :c "b"}])]
      (is (instance? Dataset dataset))
      (is (= ["a" "b" "c"] (g/column-names dataset)))
      (is (= [[1 2.0 "a"] [4 5.0 "b"]] (g/collect-vals dataset)))))
  (testing "should create the right dataset even with missing keys"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:a 1 :c "a"}
                    {:a 4 :b 5.0}])]
      (is (= ["a" "c" "b"] (g/column-names dataset)))
      (is (= [[1 "a" nil] [4 nil 5.0]] (g/collect-vals dataset)))))
  (testing "should work for bool columns"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b false}
                    {:i 1 :s "B" :b false}
                    {:i 2 :s "C" :b false}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" false]
              [1 "B" false]
              [2 "C" false]]
             (g/collect-vals dataset)))))
  (testing "should work for map columns"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b {:z ["a" "b"]}}
                    {:i 1 :s "B" :b {:z ["c" "d"]}}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" {:z ["a" "b"]}]
              [1 "B" {:z ["c" "d"]}]]
             (g/collect-vals dataset)))))
  (testing "should work for map columns with missing keys"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b {:z ["a" "b"]}}
                    {:i 1 :s "B" :b {:z ["c" "d"] :y true}}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" {:z ["a" "b"] :y nil}]
              [1 "B" {:z ["c" "d"] :y true}]]
             (g/collect-vals dataset)))))
  (testing "should work for map columns with list of maps inside"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b {:z [{:g 3}]}}
                    {:i 1 :s "B" :b {:z [{:g 5} {:h true}]}}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" {:z [{:g 3 :h nil}]}]
              [1 "B" {:z [{:g 5 :h nil} {:g nil :h true}]}]]
             (g/collect-vals dataset)))))
  (testing "should work for list of map columns"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b [{:z 1} {:z 2}]}
                    {:i 1 :s "B" :b [{:z 3}]}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" [{:z 1} {:z 2}]]
              [1 "B" [{:z 3}]]]
             (g/collect-vals dataset)))))
  (testing "should work for list of map columns with missing keys in latter entries"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b [{:z 1 :y true} {:z 2}]}
                    {:i 1 :s "B" :b [{:z 3}]}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" [{:z 1 :y true} {:z 2 :y nil}]]
              [1 "B" [{:z 3 :y nil}]]]
             (g/collect-vals dataset)))))
  (testing "should work for list of map columns with missing keys in prior entries"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :s "A" :b [{:z 1} {:z 2 :y true}]}
                    {:i 1 :s "B" :b [{:z 3}]}])]
      (is (instance? Dataset dataset))
      (is (= [[0 "A" [{:z 1 :y nil} {:z 2 :y true}]]
              [1 "B" [{:z 3 :y nil}]]]
             (g/collect-vals dataset)))))
  (testing "should work for list of list of maps with missing keys"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i 0 :b [[{:z 1} {:z 2}] [{:h true}]]}
                    {:i 1 :b [[{:g 3.0}]]}])]
      (is (instance? Dataset dataset))
      (is (= [[0 [[{:z 1 :h nil :g nil} {:z 2 :h nil :g nil}]
                  [{:z nil :h true :g nil}]]]
              [1 [[{:z nil :h nil :g 3.0}]]]]
             (g/collect-vals dataset)))))
  (testing "should work for several number of columns"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:a 1  :b 2  :c 3  :d 4  :e 5  :f 6  :g 7  :h 8  :i 9}
                    {:a 10 :b 11 :c 12 :d 13 :e 14 :f 15 :g 16 :h 17 :i 18}])]
      (is (instance? Dataset dataset))
      (is (= [{:a 1  :b 2  :c 3  :d 4  :e 5  :f 6  :g 7  :h 8  :i 9}
              {:a 10 :b 11 :c 12 :d 13 :e 14 :f 15 :g 16 :h 17 :i 18}]
             (g/collect dataset)))))
  (testing "should work for nil and empty values"
    (let [dataset (g/records->dataset
                   @tr/spark
                   [{:i nil :s []        :b []}
                    {:i nil :s [nil nil] :b []}
                    {:i nil :s nil       :b []}])]
      (is (instance? Dataset dataset))
      (is (= [[nil []        []]
              [nil [nil nil] []]
              [nil nil       []]]
             (g/collect-vals dataset))))))

(deftest inferred-types-test
  (let [day     (LocalDate/of 2026 10 1)
        instant (Instant/parse "2026-10-01T01:02:03Z")
        local   (LocalDateTime/of 2026 10 1 1 2 3)
        uuid    (UUID/fromString "6f1c2e4a-1d2b-4c3d-8e9f-0a1b2c3d4e5f")
        dataset (g/records->dataset
                 @tr/spark
                 [{:price   1.5M
                   :big     12345678901234567890N
                   :integer (BigInteger. "7")
                   :day     day
                   :sql-day (java.sql.Date/valueOf day)
                   :instant instant
                   :sql-ts  (java.sql.Timestamp/from instant)
                   :inst    (java.util.Date/from instant)
                   :local   local
                   :tag     :geni/new
                   :uuid    uuid
                   :bytes   (.getBytes "hi" "UTF-8")
                   :wait    (Duration/ofSeconds 90)
                   :term    (Period/ofMonths 14)}])]
    (testing "from the first value of each column"
      (is (= {:price   "DecimalType(38,18)"
              :big     "DecimalType(38,0)"
              :integer "DecimalType(38,0)"
              :day     "DateType"
              :sql-day "DateType"
              :instant "TimestampType"
              :sql-ts  "TimestampType"
              :inst    "TimestampType"
              :local   "TimestampNTZType"
              :tag     "StringType"
              :uuid    "StringType"
              :bytes   "BinaryType"
              :wait    "DayTimeIntervalType(0,3)"
              :term    "YearMonthIntervalType(0,1)"}
             (g/dtypes dataset))))
    (testing "with the values converted to suit"
      (let [row (first (g/collect dataset))]
        (is (== 1.5M (:price row)))
        (is (== 12345678901234567890M (:big row)))
        (is (== 7M (:integer row)))
        (is (= ["2026-10-01" "2026-10-01"] (map (comp str row) [:day :sql-day])))
        (is (= [instant instant instant]
               (map #(.toInstant ^java.util.Date (row %)) [:instant :sql-ts :inst])))
        (is (= local (:local row)))
        (is (= ["geni/new" (str uuid)] [(:tag row) (:uuid row)]))
        (is (= [(Duration/ofSeconds 90) (Period/of 1 2 0)] [(:wait row) (:term row)]))
        ;; g/collect hands back a byte array as a seq of its bytes.
        (is (= "hi" (String. (byte-array (:bytes row)) "UTF-8")))))
    (testing "with the Java 8 date and time API on too"
      ;; Classic Spark then collects java.time values, and a Spark Connect
      ;; client still collects java.sql ones.
      (let [conf      (.conf @tr/spark)
            ->instant #(if (instance? Instant %) % (.toInstant ^java.sql.Timestamp %))]
        (try
          (.set conf "spark.sql.datetime.java8API.enabled" "true")
          (let [row (first (g/collect (g/records->dataset
                                       @tr/spark
                                       [{:day     day
                                         :sql-day (java.sql.Date/valueOf day)
                                         :instant instant
                                         :inst    (java.util.Date/from instant)}])))]
            (is (= ["2026-10-01" "2026-10-01"] (map (comp str row) [:day :sql-day])))
            (is (= [instant instant] (map (comp ->instant row) [:instant :inst]))))
          (finally
            (.unset conf "spark.sql.datetime.java8API.enabled")))))
    (testing "in arrays and structs too"
      (let [row (first (g/collect (g/records->dataset @tr/spark [{:tags [:a :b] :when {:day day}}])))]
        (is (= ["a" "b"] (:tags row)))
        (is (= "2026-10-01" (str (get-in row [:when :day]))))))
    (testing "and an error that names the column for anything else"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"column \"ratio\" from a clojure.lang.Ratio"
                            (g/records->dataset @tr/spark [{:ratio 1/3}]))))))

(deftest inferred-decimals-test
  (testing "DECIMAL(38,18) for BigDecimals, and DECIMAL(38,0) for whole numbers, when they fit"
    (is (= {:x "DecimalType(38,18)" :n "DecimalType(38,0)"}
           (g/dtypes (g/records->dataset @tr/spark [{:x 1.5M :n 12345678901234567890N} {:x -2.25M :n nil}])))))
  (testing "and otherwise the digits after the point that the column needs, with room before it"
    (let [dataset (g/records->dataset @tr/spark [{:big 123456789012345678901234567890M
                                                  :fine 0.123456789012345678901234567891M}
                                                 {:big -0.5M :fine nil}])]
      (is (= {:big "DecimalType(38,8)" :fine "DecimalType(38,30)"} (g/dtypes dataset)))
      (is (= [[123456789012345678901234567890M 0.123456789012345678901234567891M] [-0.5M nil]]
             (g/collect-vals dataset)))))
  (testing "in arrays and structs too"
    (let [dataset (g/records->dataset @tr/spark [{:xs [{:a 1M}]} {:xs [{:a 0.12345678901234567890123M}]}])]
      (is (= {:xs "ArrayType(StructType(StructField(a,DecimalType(38,23),true)),true)"} (g/dtypes dataset)))
      (is (= [[{:a 1M}] [{:a 0.12345678901234567890123M}]] (g/collect-col dataset :xs)))))
  (testing "leaving out trailing zeros, which need no room"
    (let [dataset (g/records->dataset @tr/spark [{:x 1.000000000000000000000M} {:x 123456789012345678M}])]
      (is (= {:x "DecimalType(38,18)"} (g/dtypes dataset)))
      (is (= [1M 123456789012345678M] (g/collect-col dataset :x)))))
  (testing "and an error, naming the column, when no DECIMAL holds them"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"column \"x\" has numbers with up to 30 digits before the point and 10 after it"
                          (g/records->dataset @tr/spark [{:x 123456789012345678901234567890M}
                                                         {:x 0.1234567891M}])))))

(deftest table-dataset-test
  (testing "should create the right dataset"
    (let [dataset (g/table->dataset
                   @tr/spark
                   [[1 2.0 "a"]
                    [4 5.0 "b"]]
                   [:a :b :c])]
      (is (instance? Dataset dataset))
      (is (= ["a" "b" "c"] (g/column-names dataset)))
      (is (= [[1 2.0 "a"] [4 5.0 "b"]] (g/collect-vals dataset)))))
  (testing "should create the right schema for maps"
    (let [dataset (g/table->dataset
                   @tr/spark
                   [[1 {:z ["a"]}]
                    [4 {:z ["b" "c"] :y true}]]
                   [:a :b])]
      (is (instance? Dataset dataset))
      (is (= ["a" "b"] (g/column-names dataset)))
      (is (= {:a "LongType"
              :b "StructType(StructField(z,ArrayType(StringType,true),true),StructField(y,BooleanType,true))"}
             (g/dtypes dataset)))))
  (testing "should create the right schema for list of maps"
    (let [dataset (g/table->dataset
                   @tr/spark
                   [[1 [{:z 1}]]
                    [4 [{:z 3} {:y 3.0}]]]
                   [:a :b])]
      (is (instance? Dataset dataset))
      (is (= ["a" "b"] (g/column-names dataset)))
      (is (= {:a "LongType"
              :b "ArrayType(StructType(StructField(z,LongType,true),StructField(y,DoubleType,true)),true)"}
             (g/dtypes dataset)))))
  (testing "should create the right schema for list of list of maps"
    (let [dataset (g/table->dataset
                   @tr/spark
                   [[1 [[{:z 1}] [{:z 3}]]]
                    [4 [[{:y true}]]]]
                   [:a :b])]
      (is (instance? Dataset dataset))
      (is (= ["a" "b"] (g/column-names dataset)))
      (is (= {:a "LongType"
              :b "ArrayType(ArrayType(StructType(StructField(z,LongType,true),StructField(y,BooleanType,true)),true),true)"}
             (g/dtypes dataset))))))

(deftest spark-range-test
  (testing "should create simple datasets"
    (let [ds (g/range 3)]
      (is (= ["id"] (g/column-names ds)))
      (is (= [0 1 2] (g/collect ds))))
    (let [ds (g/range 3 5)]
      (is (= ["id"] (g/column-names ds)))
      (is (= [3 4] (g/collect ds))))
    (let [ds (g/range 10 20 3)]
      (is (= ["id"] (g/column-names ds)))
      (is (= [10 13 16 19] (g/collect ds))))
    (let [ds (g/range 0 100 1 5)]
      (is (= ["id"] (g/column-names ds)))
      (is (= (range 100) (g/collect ds))))))

(deftest ^:classic range-partitions-test
  (is (= 5 (count (g/partitions (g/range 0 100 1 5))))))

;; MLlib's vectors come with spark-mllib, which a Spark Connect client doesn't
;; have.
(deftest ^:classic mllib-vectors-test
  (testing "can instantiate vectors"
    (is (interop/dense-vector? (g/dense 0.0 1.0)))
    (is (interop/sparse-vector? (g/sparse 2 [1] [1.0]))))
  (testing "of vector fields"
    (let [actual (g/dtypes
                  (g/create-dataframe
                   @tr/spark
                   [(g/row (g/dense 1.0 2.0) (g/sparse 4 [1 3] [3.0 4.0]))
                    (g/row (g/dense 3.0 4.0) (g/sparse 4 [0 2] [1.0 2.0]))]
                   {:dense :vector :sparse :vector}))]
      (is (and (includes? (:dense actual) "VectorUDT")
               (includes? (:sparse actual) "VectorUDT")
               (= (set (keys actual)) #{:dense :sparse})))))
  (testing "can instantiate dataframe"
    (is (instance? Dataset (g/create-dataframe
                            @tr/spark
                            [(g/row 32 "horse" (g/dense 1.0 2.0) (g/sparse 4 [1 3] [3.0 4.0]))
                             (g/row 64 "mouse" (g/dense 3.0 4.0) (g/sparse 4 [0 2] [1.0 2.0]))]
                            (g/struct-type
                             (g/struct-field :number :integer true)
                             (g/struct-field :word :string true)
                             (g/struct-field :dense :vector true)
                             (g/struct-field :sparse :vector true))))))
  (testing "can instantiate a dataset from a table"
    (let [dataset (g/table->dataset
                   @tr/spark
                   [[0.0 (g/dense 0.5 10.0)]
                    [0.0 (g/dense 1.5 20.0)]
                    [1.0 (g/dense 1.5 30.0)]
                    [0.0 (g/dense 3.5 30.0)]
                    [0.0 (g/dense 3.5 40.0)]
                    [1.0 (g/dense 3.5 40.0)]]
                   [:label :features])]
      (is (includes? (:features (g/dtypes dataset)) "Vector")))))
