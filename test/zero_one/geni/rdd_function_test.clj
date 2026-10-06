(ns zero-one.geni.rdd-function-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.rdd.function :as function])
  (:import
   (java.io ByteArrayInputStream ByteArrayOutputStream ObjectInputStream ObjectOutputStream)
   (java.util HashSet)
   (org.apache.spark.api.java.function Function)))

(defprotocol ^:private Maker
  (make-fn [this]))

(defrecord ^:private FnMaker []
  Maker
  (make-fn [_] (fn [x] x)))

(deftest serialisability-test
  (testing "On access-field"
    (let [actual (for [field (-> HashSet .getDeclaredFields seq)]
                   (function/access-field field {}))]
      (is (and (integer? (first actual))
               (nil? (second actual))
               (not (nil? (nth actual 2)))))))
  (testing "On namespace-references"
    (is (= #{'clojure.set 'zero-one.geni.rdd-function-test}
           (function/namespace-references (fn [a b] (set/union a b)))))
    (is (= #{} (function/namespace-references clojure.lang.Keyword)))
    (testing "with vars in the collections that a function closes over"
      (let [in-vector [#'set/union]
            in-list   (java.util.ArrayList. [#'set/union])]
        (is (contains? (function/namespace-references (fn [] in-vector)) 'clojure.set))
        (is (contains? (function/namespace-references (fn [] in-list)) 'clojure.set))))
    (testing "with a function defined in a record's method"
      (is (= #{'zero-one.geni.rdd-function-test}
             (function/namespace-references (make-fn (->FnMaker))))))
    (testing "with a record in what a function closes over, whose class its namespace makes"
      (is (= #{'zero-one.geni.rdd-function-test}
             (function/namespace-references (partial identity (->FnMaker))))))
    (testing "without realising a lazy seq that a function closes over"
      (let [numbers (range)]
        (is (= #{'zero-one.geni.rdd-function-test}
               (function/namespace-references (fn [] (first numbers))))))))
  (testing "On walk-object-vars"
    (doall
     (for [obj [nil true "abc" 123 :def 'ghi (ref {})]]
       (let [refs (HashSet.)
             visited (HashSet.)]
         (function/walk-object-vars refs visited obj)
         (is (empty? (into #{} refs)))
         (is (empty? (into #{} visited))))))
    (let [refs (HashSet.)
          visited (HashSet.)]
      (function/walk-object-vars refs visited {:abc function/walk-object-vars})
      (is (not (empty? (into #{} visited))))
      (is (set/subset? #{'clojure.core} (into #{} refs))))))

(defn- round-trip [x]
  (let [out (ByteArrayOutputStream.)]
    (with-open [o (ObjectOutputStream. out)]
      (.writeObject o x))
    (with-open [in (ObjectInputStream. (ByteArrayInputStream. (.toByteArray out)))]
      (.readObject in))))

(deftest canonical-booleans-test
  (testing "falses that a function closes over, or that sit in its data, stay false"
    (let [b           false
          m           {:b false :bs [false]}
          f           (fn [_] [(if b :t :f) (if (:b m) :t :f) (if (first (:bs m)) :t :f)])
          ^Function g (round-trip (function/function f))]
      (is (= [:f :f :f] (.call g nil))))))
