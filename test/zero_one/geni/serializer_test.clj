(ns ^:classic zero-one.geni.serializer-test
  "zero_one.geni.rdd.ClojureSerializer, which Geni sets as spark.serializer on
  the local sessions it starts."
  (:require
   [clojure.test :refer [deftest is testing]])
  (:import
   (java.io ByteArrayInputStream ByteArrayOutputStream ObjectInputStream ObjectOutputStream)
   (org.apache.spark SparkConf)
   (org.apache.spark.serializer JavaSerializer)
   (scala.collection JavaConverters)
   (zero_one.geni.rdd ClojureSerializer)))

(def ^:private tag (.Any scala.reflect.ClassTag$/MODULE$))

(defn- round-trip [serializer value]
  (let [instance (.newInstance serializer)]
    (.deserialize instance (.serialize instance value tag) tag)))

(defn- canonical? [b]
  (or (identical? true b) (identical? false b)))

(def ^:private record
  {:ok false :in [false {:deep false}] :set #{false} :yes true :kw :a/b})

(deftest booleans-test
  (testing "Spark's JavaSerializer makes new Booleans, which Clojure treats as true"
    (let [read (round-trip (JavaSerializer. (SparkConf.)) record)]
      (is (= record read))
      (is (not (canonical? (:ok read))))
      (is (= :t (if (:ok read) :t :f)))))
  (testing "ClojureSerializer reads back true and false themselves"
    (let [read (round-trip (ClojureSerializer. (SparkConf.)) record)]
      (is (= record read))
      (is (every? canonical? [(:ok read)
                              (get-in read [:in 0])
                              (get-in read [:in 1 :deep])
                              (first (:set read))
                              (:yes read)]))
      (is (= :f (if (:ok read) :t :f)))
      (testing "and keywords stay interned"
        (is (identical? :a/b (:kw read)))))))

(deftest streams-test
  (testing "a stream of records reads back in order, across object stream resets"
    (let [conf     (doto (SparkConf.) (.set "spark.serializer.objectStreamReset" "7"))
          instance (.newInstance (ClojureSerializer. conf))
          values   (mapv #(hash-map :i % :even (even? %)) (range 25))
          bytes    (ByteArrayOutputStream.)
          out      (.serializeStream instance bytes)]
      (doseq [v values] (.writeObject out v tag))
      (.close out)
      (let [in   (.deserializeStream instance (ByteArrayInputStream. (.toByteArray bytes)))
            read (vec (iterator-seq (JavaConverters/asJavaIterator (.asIterator in))))]
        (is (= values read))
        (is (every? (comp canonical? :even) read)))))
  (testing "deserialising leaves the buffer's position where it was"
    (let [instance (.newInstance (ClojureSerializer. (SparkConf.)))
          buffer   (.serialize instance [1 2 3] tag)
          position (.position buffer)]
      (is (= [1 2 3] (.deserialize instance buffer tag)))
      (is (= [1 2 3] (.deserialize instance buffer (.getContextClassLoader (Thread/currentThread)) tag)))
      (is (= position (.position buffer))))))

(deftest externalizable-test
  (testing "the serializer itself travels, with its object stream reset"
    (let [conf  (doto (SparkConf.) (.set "spark.serializer.objectStreamReset" "3"))
          bytes (ByteArrayOutputStream.)]
      (with-open [out (ObjectOutputStream. bytes)]
        (.writeObject out (ClojureSerializer. conf)))
      (let [read (with-open [in (ObjectInputStream. (ByteArrayInputStream. (.toByteArray bytes)))]
                   (.readObject in))]
        (is (instance? ClojureSerializer read))
        (is (= {:ok false} (round-trip read {:ok false})))
        (is (= 3 (.get (doto (.getDeclaredField ClojureSerializer "counterReset")
                         (.setAccessible true))
                       read)))))))
