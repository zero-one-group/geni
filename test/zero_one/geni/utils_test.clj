(ns zero-one.geni.utils-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [->camel-case
                                ->kebab-case
                                ensure-coll
                                import-fn
                                import-vars]])
  (:import
   (org.apache.spark.sql.types DataTypes)
   (scala.collection Seq)))

(deftest ensure-coll-test
  (testing "should not change collections"
    (is (= [] (ensure-coll [])))
    (is (= #{"a"} (ensure-coll #{"a"})))
    (is (= {:a 1} (ensure-coll {:a 1})))
    (is (= (list 1 2) (ensure-coll (list 1 2))))
    (is (nil? (ensure-coll nil))))
  (testing "should wrap non-collections in vector"
    (is (= [1] (ensure-coll 1)))
    (is (= ["a"] (ensure-coll "a")))))

(deftest case-conversions-test
  (testing "->kebab-case splits words like camel-snake-kebab does"
    (is (= ["seller-g" "max-iter" "min-df" "input-cols" "html-parser"
            "v-2-api" "suburb" "already-kebab" "snake-case" "two-words"]
           (map ->kebab-case ["SellerG" "MaxIter" "MinDF" "inputCols" "HTMLParser"
                              "v2Api" "Suburb" "already-kebab" "snake_case" "two words"])))
    (is (= "max-iter" (->kebab-case :MaxIter)))
    (is (= "" (->kebab-case ""))))
  (testing "->camel-case"
    (is (= ["inferSchema" "timestampFormat" "header" "dateFormat"] (map ->camel-case ["infer-schema" "timestampFormat" "header" "date_format"])))
    (is (= "inferSchema" (->camel-case :infer-schema)))))

(defn source-fn
  "A docstring to carry over."
  [x]
  (inc x))

(defmacro source-macro [x] `(inc ~x))

(import-fn source-fn imported-fn)
(import-fn source-macro imported-macro)
(import-vars [clojure.string blank?])

(deftest importing-vars-test
  (testing "import-fn copies the value and the docs"
    (is (= 2 (imported-fn 1)))
    (is (= "A docstring to carry over." (-> #'imported-fn meta :doc)))
    (is (= '([x]) (-> #'imported-fn meta :arglists)))
    (is (= 'imported-fn (-> #'imported-fn meta :name)))
    (is (= (the-ns 'zero-one.geni.utils-test) (-> #'imported-fn meta :ns))))
  (testing "import-fn keeps macros as macros"
    (is (true? (-> #'imported-macro meta :macro)))
    (is (= 2 (imported-macro 1))))
  (testing "import-vars keeps the names"
    (is (blank? " "))))

(deftest java-test
  (testing "Scala Seqs"
    (is (instance? Seq (interop/->java Seq [0 1 2]))))
  (testing "numbers of the right width"
    (is (instance? Integer (interop/->java Integer/TYPE 3)))
    (let [actual (interop/->java Double/TYPE 3)]
      (is (and (instance? Double actual) (= actual 3.0))))
    (is (instance? Long (interop/->java Long 3.0))))
  (testing "arrays, including nested ones"
    (is (= [1.0 2.0] (vec (interop/->java (class (double-array 0)) [1 2]))))
    (is (= ["a" "b"] (vec (interop/->java (class (into-array String [])) ["a" "b"]))))
    (is (= [[1.0 2.0] [3.0 4.0]]
           (->> (interop/->java (class (make-array Double/TYPE 0 0)) [[1 2] [3 4]])
                (mapv vec)))))
  (testing "anything else is left alone"
    (is (= "a" (interop/->java String "a")))
    (is (true? (interop/->java Boolean/TYPE true)))
    (is (= DataTypes/StringType (interop/->java Object DataTypes/StringType)))))

(deftest clojure-test
  (let [data      [(interop/->scala-seq [1 2 3])]
        converted (interop/->clojure data)]
    (is (= (map interop/->clojure data) converted))))
