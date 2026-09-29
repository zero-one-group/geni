(ns zero-one.geni.utils-test
  (:require
   [clojure.string]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [->camel-case
                                ->kebab-case
                                ensure-coll
                                import-fn
                                import-vars
                                with-dynamic-import]])
  (:import
   (org.apache.spark.sql.types DataTypes)
   (scala.collection Seq)))

(facts "On dynamic imports"
  (fact "succeeds with valid import forms"
    (with-dynamic-import
      [[org.apache.spark.sql functions]]
      (def adf-def 123)) => #(and (= % :succeeded) (= adf-def 123))
    (with-dynamic-import
      [org.apache.spark.sql.Column]
      (def ghi-jkl 123)) => :succeeded) ;#(and (= % :succeeded) (= ghi-jkl 123))))
  (fact "fails gracefully"
    (with-dynamic-import
      [[some.non-existent.namespace non-existent-class]]
      (def mno-pqr 123)) => #(and (= % :failed) (nil? (resolve 'mno-pqr)))
    (with-dynamic-import
      [some.non-existent.namespace.NonExistentClass]
      (def stu-vwx 123)) => #(and (= % :failed) (nil? (resolve 'stu-vwx)))
    (with-dynamic-import
      (+ 1 1)
      (def xyz 123)) => #(and (= % :failed) (nil? (resolve 'xyz)))))

(facts "On ensure-coll"
  (fact "should not change collections"
    (ensure-coll []) => []
    (ensure-coll #{"a"}) => #{"a"}
    (ensure-coll {:a 1}) => {:a 1}
    (ensure-coll (list 1 2)) => (list 1 2)
    (ensure-coll nil) => nil)
  (fact "should wrap non-collections in vector"
    (ensure-coll 1) => [1]
    (ensure-coll "a") => ["a"]))

(facts "On case conversions"
  (fact "->kebab-case splits words like camel-snake-kebab does"
    (map ->kebab-case ["SellerG" "MaxIter" "MinDF" "inputCols" "HTMLParser"
                       "v2Api" "Suburb" "already-kebab" "snake_case" "two words"])
    => ["seller-g" "max-iter" "min-df" "input-cols" "html-parser"
        "v-2-api" "suburb" "already-kebab" "snake-case" "two-words"]
    (->kebab-case :MaxIter) => "max-iter"
    (->kebab-case "") => "")
  (fact "->camel-case"
    (map ->camel-case ["infer-schema" "timestampFormat" "header" "date_format"])
    => ["inferSchema" "timestampFormat" "header" "dateFormat"]
    (->camel-case :infer-schema) => "inferSchema"))

(defn source-fn
  "A docstring to carry over."
  [x]
  (inc x))

(defmacro source-macro [x] `(inc ~x))

(import-fn source-fn imported-fn)
(import-fn source-macro imported-macro)
(import-vars [clojure.string blank?])

(facts "On importing vars"
  (fact "import-fn copies the value and the docs"
    (imported-fn 1) => 2
    (-> #'imported-fn meta :doc) => "A docstring to carry over."
    (-> #'imported-fn meta :arglists) => '([x])
    (-> #'imported-fn meta :name) => 'imported-fn
    (-> #'imported-fn meta :ns) => (the-ns 'zero-one.geni.utils-test))
  (fact "import-fn keeps macros as macros"
    (-> #'imported-macro meta :macro) => true
    (imported-macro 1) => 2)
  (fact "import-vars keeps the names"
    (blank? " ") => true))

(facts "On ->java"
  (fact "Scala Seqs"
    (interop/->java Seq [0 1 2]) => #(instance? Seq %))
  (fact "numbers of the right width"
    (interop/->java Integer/TYPE 3) => #(instance? Integer %)
    (interop/->java Double/TYPE 3) => #(and (instance? Double %) (= % 3.0))
    (interop/->java Long 3.0) => #(instance? Long %))
  (fact "arrays, including nested ones"
    (vec (interop/->java (class (double-array 0)) [1 2])) => [1.0 2.0]
    (vec (interop/->java (class (into-array String [])) ["a" "b"])) => ["a" "b"]
    (->> (interop/->java (class (make-array Double/TYPE 0 0)) [[1 2] [3 4]])
         (mapv vec)) => [[1.0 2.0] [3.0 4.0]])
  (fact "anything else is left alone"
    (interop/->java String "a") => "a"
    (interop/->java Boolean/TYPE true) => true
    (interop/->java Object DataTypes/StringType) => DataTypes/StringType))

(fact "On ->clojure"
  (let [data      [(interop/->scala-seq [1 2 3])]
        converted (interop/->clojure data)]
    converted => (map interop/->clojure data)))
