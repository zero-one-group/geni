(ns zero-one.geni.udf-test
  "Spark SQL UDFs from Clojure functions, on a local session and over Spark
  Connect, whose test server has no Clojure."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.core.udf :as udf]
   [zero-one.geni.core.udf-artifacts :as udf-artifacts]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :as tr :refer [spark]])
  (:import
   (clojure.lang ExceptionInfo RT)
   (java.io ByteArrayInputStream ByteArrayOutputStream ObjectInputStream ObjectOutputStream
            ObjectStreamClass)
   (java.sql Timestamp)
   (java.time Instant LocalDate)
   (java.util.concurrent ExecutionException)
   (org.apache.spark.sql SparkSession)
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
  (testing ":nullable reaches Spark"
    (let [df (g/select (ids 1) {:x ((g/udf inc :long {:nullable false}) :id)
                                :y ((g/udf inc :long) :id)})]
      (is (= [false true] (map #(.nullable %) (.fields (.schema df)))))))
  (testing "a Spark DataType works as the return type"
    (is (= [[1]] (select-vals (ids 1) ((g/udf inc DataTypes/LongType) :id))))))

(deftest ^:classic deterministic-udf-test
  (let [df (g/select (ids 1) {:x ((g/udf inc :long {:deterministic false}) :id)
                              :y ((g/udf inc :long) :id)})
        [x y] (-> df .queryExecution .analyzed .expressions interop/scala-seq->vec)]
    (is (= [false true] [(.deterministic x) (.deterministic y)]))))

(deftest udf-errors-test
  (is (thrown-with-msg? ExceptionInfo #":lonng isn't a Spark type"
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

(deftest var-udf-test
  (testing "a var goes by name, and the executors, or the server, load its namespace"
    (is (= [[0] [2]] (select-vals (ids 2) ((g/udf #'doubled :long) :id))))))

(deftest datetime-udf-test
  (testing "dates and timestamps go out from java.time or java.sql values"
    (let [day     (LocalDate/of 2026 1 2)
          instant (Instant/parse "2026-01-02T03:04:05Z")
          [[d1 d2 t1 t2]]
          (select-vals (ids 1)
                       ((g/udf (fn [_] day) :date) :id)
                       ((g/udf (fn [_] (java.sql.Date/valueOf day)) :date) :id)
                       ((g/udf (fn [_] instant) :timestamp) :id)
                       ((g/udf (fn [_] (Timestamp/from instant)) :timestamp) :id))]
      (is (= [day day] (map #(.toLocalDate ^java.sql.Date %) [d1 d2])))
      (is (= [instant instant] (map #(.toInstant ^Timestamp %) [t1 t2]))))))

(deftest uses-test
  (testing "the namespaces that a namespace's file requires, with an alias or without"
    (is (contains? (#'udf-artifacts/uses 'zero-one.geni.udf-fixture) 'clojure.set)))
  (testing "an ns form's libs, from prefix lists too, but not those with :as-alias alone"
    (is (= '#{a b.c b.d e g f h.i}
           (#'udf-artifacts/required-libs
            '(ns x "A namespace." {:k 1}
                 (:require a [b c [d :as d]] [e :as e] [i :as-alias i] [g :as-alias g :refer [x]]
                           :reload)
                 (:use f [h i])
                 (:import (java.io File))))))))

(defn- define-at-repl
  "Evaluates `form` in a namespace without a file, as the REPL would."
  [form]
  (binding [*ns* (create-ns 'zero-one.geni.udf-test-repl)]
    (refer-clojure)
    (eval form)))

(defn- unwrap [f]
  (try (f) (catch ExecutionException e (throw (.getCause e)))))

(defn- keeping-default
  "Calls `f`, and then makes the default session the default again, since
  g/connect makes each new session the default."
  [f]
  (let [session @spark]
    (try
      (f)
      (finally
        (SparkSession/setDefaultSession session)
        (SparkSession/setActiveSession session)))))

(deftest ^:connect connect-udf-test
  (testing "a function compiled at the REPL after g/connect goes to the server"
    (let [triple (eval '(fn [x] (* 3 x)))]
      (is (= [[0] [3]] (select-vals (ids 2) ((g/udf triple :long) :id))))))
  (testing "a function compiled without its class kept gets an error that says what to do"
    (let [unkept (binding [*compile-files* false] (eval '(fn [x] (* 4 x))))]
      (is (thrown-with-msg? ExceptionInfo #"was compiled at run time.*before g/connect's :keep-classes"
                            (g/udf unkept :long)))))
  (testing "a var of a namespace without a file gets an error, since it goes by name"
    (is (thrown-with-msg? ExceptionInfo #"has no file for the server to load"
                          (g/udf (define-at-repl '(defn same [x] x)) :long)))))

(deftest ^:connect keep-classes-test
  (keeping-default
   (fn []
     (testing "g/connect keeps classes only when it's asked to"
       (let [calls (atom 0)]
         (with-redefs [udf-artifacts/keep-classes! #(swap! calls inc)]
           (.close (g/connect))
           (.close (g/connect nil {:keep-classes true})))
         (is (= 1 @calls))))
     (testing "from a future, which has its caller's bindings, it throws before it connects"
       (is (thrown-with-msg? ExceptionInfo #"another thread's bindings"
                             (binding [*compile-path* *compile-path*]
                               (unwrap #(deref (future (g/connect nil {:keep-classes true})))))))))))

(deftest ^:connect changed-function-test
  (let [old (define-at-repl '(defn shift [x] (+ x 1)))
        u   (g/udf @old :long)]
    (is (= [[1] [2]] (select-vals (ids 2) (u :id))))
    (let [new @(define-at-repl '(defn shift [x] (+ x 2)))]
      (testing "a function redefined after its class went to the session throws"
        (is (thrown-with-msg? ExceptionInfo #"udf-test-repl/shift.*changed after it went"
                              (g/udf new :long))))
      (testing "while the UDF of the function as it was still works"
        (is (= [[1] [2]] (select-vals (ids 2) (u :id))))))))

(deftest ^:connect new-session-udf-test
  ;; One new session, which the server loads Clojure and Geni for, for the
  ;; checks that need a session that nothing has gone to yet. It isn't the
  ;; default one, so the UDFs have to find it.
  (let [default @spark]
    (with-open [fresh (g/connect)]
      (SparkSession/setDefaultSession default)
      (SparkSession/setActiveSession default)
      ;; Loaded without its classes kept, as before g/connect, and then added
      ;; to at the REPL, so that only some of its classes are kept.
      (binding [*compile-files* false]
        (require 'zero-one.geni.udf-fixture :reload))
      (let [on-fresh    #(g/collect-vals (g/select (g/range fresh 2) {:x %}))
            plus-offset (resolve 'zero-one.geni.udf-fixture/plus-offset)
            new-keys    @(resolve 'zero-one.geni.udf-fixture/new-keys)
            twice       (binding [*ns* (the-ns 'zero-one.geni.udf-fixture)]
                          (eval '(fn [x] (* 2 (plus-offset x)))))]
        (testing "a UDF goes to each open session, and the server loads a namespace from its file"
          (is (= [[200] [202]] (on-fresh ((g/udf twice :long) :id))))
          (is (= [[100] [101]] (on-fresh ((g/udf plus-offset :long) :id))))
          (is (= [[["b"]] [["b"]]]
                 (on-fresh ((g/udf (fn [x] (new-keys {"a" x "b" x} #{"a"})) [:string]) :id))))
          (let [offset ((resolve 'zero-one.geni.udf-fixture/->Offset) 5)]
            (is (= [[5] [6]] (on-fresh ((g/udf (fn [x] (+ x (:n offset))) :long) :id))))))
        (testing "a file that changed after it went to the session throws"
          (let [u      (g/udf plus-offset :long)
                file   (io/file "test/zero_one/geni/udf_fixture.clj")
                before (.lastModified file)]
            (try
              (.setLastModified file (+ (System/currentTimeMillis) 60000))
              (is (thrown-with-msg? ExceptionInfo #"udf-fixture.*changed after it went"
                                    (u :id)))
              (finally
                (.setLastModified file before)))))))))

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
