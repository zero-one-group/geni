(ns ^:classic zero-one.geni.udf-test
  "Spark SQL UDFs from Clojure functions."
  (:require
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.core.udf :as udf]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :refer [spark]])
  (:import
   (clojure.lang ExceptionInfo RT)
   (java.io ByteArrayInputStream ByteArrayOutputStream ObjectInputStream ObjectOutputStream
            ObjectStreamClass)
   (org.apache.spark.sql.api.java UDF1)
   (org.apache.spark.sql.types DataTypes)))

;; Spark evaluates a UDF over an in-memory table on the driver, so the tests
;; use g/range and g/repartition, whose tasks go through serialisation.
(defn- ids [n]
  (g/range n))

(defn- select-vals [dataframe & exprs]
  (-> dataframe (g/select (apply array-map (interleave (map #(keyword (str "c" %)) (range)) exprs)))
      g/collect-vals))

(deftest scalar-udf-test
  (testing "results come back as the declared type"
    (let [df (-> (ids 3)
                 (g/select {:long ((g/udf inc :long) :id)
                            :int  ((g/udf #(* 10 %) :int) :id)
                            :dbl  ((g/udf #(/ % 2) :double) :id)
                            :str  ((g/udf #(keyword (str "k" %)) :string) :id)
                            :bool ((g/udf odd? :boolean) :id)}))]
      (is (= {:long "LongType" :int "IntegerType" :dbl "DoubleType" :str "StringType"
              :bool "BooleanType"}
             (g/dtypes df)))
      (is (= [[1 0 0.0 "k0" false] [2 10 0.5 "k1" true] [3 20 1.0 "k2" false]]
             (g/collect-vals df)))))
  (testing "a null goes in as nil, and nil comes out as a null"
    (is (= [[nil nil] [11 nil] [12 2]]
           (-> (g/table->dataset @spark [[nil] [1] [2]] [:x])
               (g/repartition 2)
               (g/select {:plus-ten ((g/udf #(some-> % (+ 10)) :long) :x)
                          :evens    ((g/udf #(when (and % (even? %)) %) :long) :x)})
               (g/order-by :x)
               g/collect-vals))))
  (testing "a false that the function closes over stays false"
    (let [b false
          m {:b false}]
      (is (= [["f" "f"]]
             (select-vals (ids 1)
                          ((g/udf (fn [_] (if b "t" "f")) :string) :id)
                          ((g/udf (fn [_] (if (:b m) "t" "f")) :string) :id))))))
  (testing "several columns, and none"
    (is (= [[0 7] [2 7]]
           (select-vals (ids 2) ((g/udf + :long) :id :id) ((g/udf (constantly 7) :int)))))))

(deftest collection-udf-test
  (testing "arrays come in as seqs, and go out from any collection"
    (is (= [[3 [0 1 2]]]
           (select-vals (ids 1)
                        ((g/udf count :int) (g/array (g/lit 1) (g/lit 2) (g/lit 3)))
                        ((g/udf #(range (+ 3 %)) [:long]) :id)))))
  (testing "maps come in with Spark's keys, and go out from Clojure maps"
    (is (= [[1 {"a" 0 "b" 1}]]
           (select-vals (ids 1)
                        ((g/udf #(get % "a") :long) (g/map (g/lit "a") (g/lit 1)))
                        ((g/udf (fn [x] {:a x :b (inc x)}) [:string :long]) :id)))))
  (testing "structs come in as maps, and go out from maps or from values in order"
    (let [df (-> (ids 2)
                 (g/select {:sum   ((g/udf #(+ (:a %) (:b %)) :long) (g/struct {:a :id :b :id}))
                            :named ((g/udf (fn [x] {:n x "s" (str x)}) {:n :long :s :string}) :id)
                            :order ((g/udf (fn [x] [x (str x)]) {:n :long :s :string}) :id)}))]
      (is (= [{:sum 0 :named {:n 0 :s "0"} :order {:n 0 :s "0"}}
              {:sum 2 :named {:n 1 :s "1"} :order {:n 1 :s "1"}}]
             (g/collect df))))))

(deftest udf-options-test
  (testing ":name names the column"
    (is (= ["plus_one(id)"]
           (g/column-names (g/select (ids 1) ((g/udf inc :long {:name "plus_one"}) :id))))))
  (testing ":deterministic and :nullable reach Spark"
    (let [df (g/select (ids 1) {:x ((g/udf inc :long {:deterministic false :nullable false}) :id)
                                :y ((g/udf inc :long) :id)})
          [x y] (-> df .queryExecution .analyzed .expressions interop/scala-seq->vec)]
      (is (= [false true] [(.deterministic x) (.deterministic y)]))
      (is (= [false true] (map #(.nullable %) (.fields (.schema df)))))))
  (testing "a Spark DataType works as the return type"
    (is (= [[1]] (select-vals (ids 1) ((g/udf inc DataTypes/LongType) :id))))))

(deftest udf-errors-test
  (is (thrown-with-msg? ExceptionInfo #"Unknown UDF return type :lonng"
                        (g/udf inc :lonng)))
  (is (thrown-with-msg? ExceptionInfo #"up to 10 columns"
                        ((g/udf + :long) :a :b :c :d :e :f :g :h :i :j :k))))

(deftest register-udf-test
  (testing "a registered UDF works in SQL and in g/expr, with the arity of its function"
    (let [plus-one (g/register-udf! "plus_one" inc :long)]
      (g/create-or-replace-temp-view! (ids 2) "udf_ids")
      (is (= [[1] [2]] (g/collect-vals (g/sql @spark "SELECT plus_one(id) FROM udf_ids ORDER BY id"))))
      (is (= [[1] [2]] (select-vals (ids 2) (g/expr "plus_one(id)"))))
      (is (= [[1] [2]] (select-vals (ids 2) (plus-one :id))))
      (is (thrown-with-msg? ExceptionInfo #"plus_one takes 1 columns, and got 2"
                            (plus-one :id :id)))))
  (testing "a function of several arities needs :arity, on the default session or a given one"
    (is (thrown-with-msg? ExceptionInfo #"Pass :arity"
                          (g/register-udf! "add" + :long)))
    (g/register-udf! @spark :add + :long {:arity 3})
    (is (= [[1] [3]] (select-vals (ids 2) (g/expr "add(id, id, 1)"))))))

(defn- round-trip [x]
  (let [out (ByteArrayOutputStream.)]
    (with-open [o (ObjectOutputStream. out)]
      (.writeObject o x))
    ;; Resolves classes as Clojure does, which also finds the test's own fns.
    (with-open [in (proxy [ObjectInputStream] [(ByteArrayInputStream. (.toByteArray out))]
                     (resolveClass [desc]
                       (RT/classForName (.getName ^ObjectStreamClass desc))))]
      (.readObject in))))

(defn- doubled [x] (* 2 x))

(defn- plain-round-trip
  "As `round-trip`, but with Java's own class resolution, which doesn't find
  functions compiled at run time."
  [x]
  (let [out (ByteArrayOutputStream.)]
    (with-open [o (ObjectOutputStream. out)]
      (.writeObject o x))
    (with-open [in (ObjectInputStream. (ByteArrayInputStream. (.toByteArray out)))]
      (.readObject in))))

(deftest serialisation-test
  (testing "a var travels by name, so its function's class isn't needed"
    (let [^UDF1 f (plain-round-trip (#'udf/->udf-fn #'doubled DataTypes/LongType))]
      (is (= 42 (.call f 21)))))
  (let [udf-fn #'udf/->udf-fn
        ^UDF1 f (round-trip (udf-fn #(* 2 %) DataTypes/IntegerType))]
    (testing "only the function and the type travel, and the converter is rebuilt"
      (is (= (int 42) (.call f 21)))
      (is (instance? Integer (.call f 21))))
    (testing "the executors load the function's namespace"
      (is (some #(string/includes? % "zero-one.geni.udf-test")
                (.get (doto (.getDeclaredField zero_one.geni.rdd.function.SerializableFn "namespaces")
                        (.setAccessible true))
                      f))))))
