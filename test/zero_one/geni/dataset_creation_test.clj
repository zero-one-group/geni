(ns zero-one.geni.dataset-creation-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g])
  (:import
    (org.apache.spark.sql Dataset
                          Row)
    (org.apache.spark.sql.types StructField
                                StructType)
    (org.apache.spark.ml.linalg DenseVector
                                SparseVector)))

(facts "On creation of empty dataset" :empty-dataset
  (fact "correct creation"
    (g/create-dataframe [] {}) => g/empty?
    (g/table->dataset [] []) => g/empty?
    (g/map->dataset {}) => g/empty?
    (g/records->dataset {}) => g/empty?)
  (fact "correct schema"
    (g/dtypes (g/create-dataframe [] {:i :int})) => {:i "IntegerType"}
    (g/dtypes
      (g/create-dataframe [] (g/struct-type (g/struct-field :j :float true))))
    => {:j "FloatType"}))

(fact "can instantiate dataframe with data-oriented schema" :schema
  (g/dtypes
    (g/create-dataframe
      [(g/row 32 "horse" (g/dense 1.0 2.0) (g/sparse 4 [1 3] [3.0 4.0]))
       (g/row 64 "mouse" (g/dense 3.0 4.0) (g/sparse 4 [0 2] [1.0 2.0]))]
      {:number :int
       :word :string
       :dense :vector
       :sparse :vector}))
  => #(and (= (:number %) "IntegerType")
           (= (:word %) "StringType")
           (includes? (:dense %) "VectorUDT")
           (includes? (:sparse %) "VectorUDT")
           (= (set (keys %)) #{:dense :number :sparse :word})))

(facts "On building blocks"
  (fact "can instantiate vectors"
    (g/dense 0.0 1.0) => #(instance? DenseVector %)
    (g/sparse 2 [1] [1.0]) => #(instance? SparseVector %)
    (g/row [2]) => #(instance? Row %))
  (fact "can instantiate struct field and type"
    (let [field (g/struct-field :number :integer true)]
      field => #(instance? StructField %)
      (g/struct-type field) => #(instance? StructType %)))
  (fact "can instantiate dataframe"
    (g/create-dataframe
      [(g/row 32 "horse" (g/dense 1.0 2.0) (g/sparse 4 [1 3] [3.0 4.0]))
       (g/row 64 "mouse" (g/dense 3.0 4.0) (g/sparse 4 [0 2] [1.0 2.0]))]
      (g/struct-type
        (g/struct-field :number :integer true)
        (g/struct-field :word :string true)
        (g/struct-field :dense :vector true)
        (g/struct-field :sparse :vector true)))
    => #(instance? Dataset %))
  (fact "can instantiate example dataframes"
    (let [expected-dtypes {:number "LongType" :word "StringType"}]
      (g/dtypes
        (g/to-df
          [[8 "bat"] [64 "mouse"] [-27 "horse"]]
          [:number :word])) => expected-dtypes
      (g/dtypes
        (g/create-dataframe
          [(g/row 8 "bat")
           (g/row 64 "mouse")
           (g/row -27 "horse")]
          (g/struct-type
            (g/struct-field :number :long true)
            (g/struct-field :word :string true)))) => expected-dtypes
      (g/dtypes
        (g/table->dataset
          [[8 "bat"] [64 "mouse"] [-27 "horse"]]
          [:number :word])) => expected-dtypes
      (g/dtypes
        (g/map->dataset
          {:number [8 64 -27]
           :word   ["bat" "mouse" "horse"]})) => expected-dtypes
      (g/dtypes
        (g/records->dataset
          [{:number   8 :word "bat"}
           {:number  64 :word "mouse"}
           {:number -27 :word "horse"}])) => expected-dtypes)))

(facts "On map->dataset"
  (fact "should create the right dataset"
    (let [dataset (g/map->dataset
                    {:a [1 4]
                     :b [2.0 5.0]
                     :c ["a" "b"]})]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]]))
  (fact "should create the right schema even with nils"
    (let [dataset (g/map->dataset
                    {:a [nil 4]
                     :b [2.0 5.0]})]
      (g/collect-vals dataset) => [[nil 2.0] [4 5.0]]))
  (fact "should create the right null column"
    (let [dataset (g/map->dataset
                    {:a [1 4]
                     :b [nil nil]})]
      (g/collect-vals dataset) => [[1 nil] [4 nil]]))
  (let [dataset (g/table->dataset
                   [[0.0 (g/dense 0.5 10.0)]
                    [0.0 (g/dense 1.5 20.0)]
                    [1.0 (g/dense 1.5 30.0)]
                    [0.0 (g/dense 3.5 30.0)]
                    [0.0 (g/dense 3.5 40.0)]
                    [1.0 (g/dense 3.5 40.0)]]
                   [:label :features])]
    (:features (g/dtypes dataset)) => #(includes? % "Vector")))

(facts "On records->dataset"
  (fact "should create the right dataset"
    (let [dataset (g/records->dataset
                    [{:a 1 :b 2.0 :c "a"}
                     {:a 4 :b 5.0 :c "b"}])]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]]))
  (fact "should create the right dataset even with missing keys"
    (let [dataset (g/records->dataset
                    [{:a 1 :c "a"}
                     {:a 4 :b 5.0}])]
      (g/column-names dataset) => ["a" "c" "b"]
      (g/collect-vals dataset) => [[1 "a" nil] [4 nil 5.0]])))

(facts "On table->dataset"
  (fact "should create the right dataset"
    (let [dataset (g/table->dataset
                    [[1 2.0 "a"]
                     [4 5.0 "b"]]
                    [:a :b :c])]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]])))
