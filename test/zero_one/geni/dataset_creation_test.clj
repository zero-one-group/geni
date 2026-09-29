(ns zero-one.geni.dataset-creation-test
  (:require
   [clojure.string :refer [includes?]]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :as tr])
  (:import
   (org.apache.spark.sql Dataset
                         Row)
   (org.apache.spark.sql.types StructField
                               StructType)
   (org.apache.spark.ml.linalg DenseVector
                               SparseVector)))

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
             [(g/row 32 "horse")
              (g/row 64 "mouse")]
             {:number :int :word :str})))))
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
  (testing "of struct fields"
    (is (= {:coord "StructType(StructField(x,IntegerType,true),StructField(y,IntegerType,true))"}
           (g/dtypes
            (g/create-dataframe
             @tr/spark
             [(g/row (g/row 27 42))
              (g/row (g/row 57 18))]
             {:coord {:x :int :y :int}})))))
  (testing "of struct array fields"
    (is (= {:coords "ArrayType(StructType(StructField(x,IntegerType,true),StructField(y,IntegerType,true)),true)"}
           (g/dtypes
            (g/create-dataframe
             @tr/spark
             [(g/row [(g/row 27 42)])
              (g/row [(g/row 57 18)])]
             {:coords [{:x :int :y :int}]}))))))

(deftest building-blocks-test
  (testing "can instantiate vectors"
    (is (instance? DenseVector (g/dense 0.0 1.0)))
    (is (instance? SparseVector (g/sparse 2 [1] [1.0])))
    (is (instance? Row (g/row [2]))))
  (testing "can instantiate struct field and type"
    (let [field (g/struct-field :number :integer true)]
      (is (instance? StructField field))
      (is (instance? StructType (g/struct-type field)))))
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
      (is (= [[1 nil] [4 nil]] (g/collect-vals dataset)))))
  (let [dataset (g/table->dataset
                 @tr/spark
                 [[0.0 (g/dense 0.5 10.0)]
                  [0.0 (g/dense 1.5 20.0)]
                  [1.0 (g/dense 1.5 30.0)]
                  [0.0 (g/dense 3.5 30.0)]
                  [0.0 (g/dense 3.5 40.0)]
                  [1.0 (g/dense 3.5 40.0)]]
                 [:label :features])]
    (is (includes? (:features (g/dtypes dataset)) "Vector"))))

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
      (is (= (range 100) (g/collect ds)))
      (is (= 5 (count (g/partitions ds)))))))
