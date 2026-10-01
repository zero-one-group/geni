(ns zero-one.geni.results-tmd-test
  "g/to-tmd, g/stream, g/to-tensors, g/stream-tensors and g/create-dataframe
  with tech.ml.dataset, on classic Spark and over Spark Connect."
  (:require
   [clojure.test :refer [deftest is testing]]
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.column :as ds-col]
   [tech.v3.datatype :as dtype]
   [tech.v3.tensor :as dtt]
   [zero-one.geni.core :as g]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.test-resources :as tr]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang ExceptionInfo Reflector)
   (java.time Duration Instant LocalDate LocalDateTime LocalTime Period)))

(defn- spark-4? [] (boolean (re-find #"^4\." (g/version))))

(defn- classic? [] (spark/classic-session? @tr/spark))

(defn- datatypes [dataset]
  (into {} (map (juxt (comp :name meta) (comp :datatype meta))) (vals dataset)))

(defn- simple-types
  "The Spark type of each column, without whether it's nullable."
  [df]
  (into {} (map (fn [^org.apache.spark.sql.types.StructField field]
                  [(keyword (.name field)) (.simpleString (.dataType field))]))
        (.fields (.schema df))))

(defn- missing [dataset k]
  (vec (ds-col/missing (dataset k))))

(def ^:private types-sql
  (str "SELECT id, CAST(id AS INT) i, CAST(id AS SHORT) s, CAST(id AS BYTE) b, id * 1.5D d, "
       "CAST(id AS FLOAT) f, id % 2 = 0 bool, CONCAT('s', id) str, "
       "CAST(id AS DECIMAL(10, 2)) dec, DATE'2026-01-01' + CAST(id AS INT) day, "
       "TIMESTAMP'2026-01-01 12:00:00' ts, TIMESTAMP_NTZ'2026-01-01 12:00:00' ntz, "
       "CAST(CONCAT('b', id) AS BINARY) bin, ARRAY(id, id + 1) arr, ARRAY('a', NULL) strs, "
       "NAMED_STRUCT('x', id, 'day', DATE'2026-01-02') st, MAP('k', id) m, NULL nothing, "
       "INTERVAL '1 02:03:04.5' DAY TO SECOND wait, INTERVAL '1-2' YEAR TO MONTH term, "
       "IF(id = 1, NULL, id) maybe FROM RANGE(3)"))

(defmacro ^:private with-session-time-zone
  "Runs `body` with the session's time zone set to `zone`."
  [zone & body]
  `(let [conf# (.conf @tr/spark)]
     (.set conf# "spark.sql.session.timeZone" ~zone)
     (try ~@body (finally (.unset conf# "spark.sql.session.timeZone")))))

(deftest to-tmd-types-test
  ;; Seven hours ahead of UTC, so that a TIMESTAMP and a TIMESTAMP_NTZ differ.
  (with-session-time-zone "Asia/Jakarta"
    (let [dataset (g/to-tmd (g/sql @tr/spark types-sql))]
      (testing "a column per Spark column, with keyword names"
        (is (= [3 21] [(ds/row-count dataset) (ds/column-count dataset)]))
        (is (= (map keyword (g/column-names (g/sql @tr/spark types-sql)))
               (ds/column-names dataset))))
      (testing "with tech.ml.dataset's datatypes"
        (is (= {:id      :int64
                :i       :int32
                :s       :int16
                :b       :int8
                :d       :float64
                :f       :float32
                :bool    :boolean
                :str     :string
                :dec     :decimal
                :day     :packed-local-date
                :ts      :packed-instant
                :ntz     :local-date-time
                :bin     :object
                :arr     :persistent-vector
                :strs    :persistent-vector
                :st      :persistent-map
                :m       :persistent-map
                :nothing :object
                :wait    :packed-duration
                :term    :object
                :maybe   :int64}
               (datatypes dataset))))
      (testing "and the values"
        (is (= [0 1 2] (vec (dataset :id))))
        (is (= [0.0 1.5 3.0] (vec (dataset :d))))
        (is (= [0.0 1.0 2.0] (vec (dataset :f))))
        (is (= [true false true] (vec (dataset :bool))))
        (is (= ["s0" "s1" "s2"] (vec (dataset :str))))
        (is (= [0.00M 1.00M 2.00M] (vec (dataset :dec))))
        (is (= (map #(LocalDate/of 2026 1 %) [1 2 3]) (vec (dataset :day))))
        (is (= (repeat 3 (Instant/parse "2026-01-01T05:00:00Z")) (vec (dataset :ts))))
        (is (= (repeat 3 (LocalDateTime/of 2026 1 1 12 0)) (vec (dataset :ntz))))
        (is (= ["b0" "b1" "b2"] (map #(String. ^bytes % "UTF-8") (dataset :bin))))
        (is (= [[0 1] [1 2] [2 3]] (vec (dataset :arr))))
        (is (= (repeat 3 ["a" nil]) (vec (dataset :strs))))
        (is (= (for [x [0 1 2]] {:x x :day (LocalDate/of 2026 1 2)}) (vec (dataset :st))))
        (is (= [{"k" 0} {"k" 1} {"k" 2}] (vec (dataset :m))))
        (is (= (repeat 3 (Duration/parse "PT26H3M4.5S")) (vec (dataset :wait))))
        (is (= (repeat 3 (Period/of 1 2 0)) (vec (dataset :term)))))
      (testing "with nulls as missing values"
        (is (= [0 nil 2] (vec (dataset :maybe))))
        (is (= [1] (missing dataset :maybe)))
        (is (= [0 1 2] (missing dataset :nothing)))
        (is (= [] (missing dataset :id))))))
  (testing "VARIANT, as Spark's VariantVal"
    (when (spark-4?)
      (let [value (first ((g/to-tmd (g/sql @tr/spark "SELECT PARSE_JSON('{\"a\": [1, 2]}') v")) :v))]
        (is (= "org.apache.spark.unsafe.types.VariantVal" (.getName (class value))))
        (is (= "{\"a\":[1,2]}" (str value))))))
  (testing "TIME, as a LocalTime, on classic Spark 4.1 and later, which can make one"
    (when (and (classic?) (spark-4?) (not (re-find #"^4\.0\." (g/version))))
      (is (= [(LocalTime/parse "12:34:56.789")]
             (vec ((g/to-tmd (g/sql @tr/spark "SELECT TIME'12:34:56.789' t")) :t))))))
  (testing "column names through :key-fn"
    (is (= ["id"] (ds/column-names (g/to-tmd (g/range 2) {:key-fn identity}))))))

(deftest to-tmd-edge-cases-test
  (testing "an empty result has the columns and their datatypes, and no rows"
    (let [dataset (g/to-tmd (g/limit (g/sql @tr/spark types-sql) 0))]
      (is (= [0 21] [(ds/row-count dataset) (ds/column-count dataset)]))
      (is (= {:id :int64 :str :string :ts :packed-instant :arr :persistent-vector}
             (select-keys (datatypes dataset) [:id :str :ts :arr])))))
  (testing "columns of nulls keep their datatypes"
    (let [dataset (g/to-tmd (g/sql @tr/spark (str "SELECT CAST(NULL AS INT) i, CAST(NULL AS STRING) s, "
                                                  "CAST(NULL AS DOUBLE) d, CAST(NULL AS ARRAY<INT>) a "
                                                  "FROM RANGE(2)")))]
      (is (= {:i :int32 :s :string :d :float64 :a :persistent-vector} (datatypes dataset)))
      (is (every? #(= [0 1] (missing dataset %)) [:i :s :d :a]))
      (is (every? #(= [nil nil] (vec (dataset %))) [:i :s :d :a]))))
  (testing "decimals keep every digit"
    (let [dataset (g/to-tmd (g/sql @tr/spark (str "SELECT CAST('12345678901234567890.123456789012345678' "
                                                  "AS DECIMAL(38, 18)) big, CAST(-1.5 AS DECIMAL(3, 1)) neg")))]
      (is (= [12345678901234567890.123456789012345678M] (vec (dataset :big))))
      (is (= [-1.5M] (vec (dataset :neg))))))
  (testing "batches become one dataset, in order"
    (let [dataset (g/to-tmd (g/sql @tr/spark "SELECT id, IF(id % 3 = 0, NULL, id) x FROM RANGE(0, 9, 1, 3)"))]
      (is (= (range 9) (vec (dataset :id))))
      (is (= [0 3 6] (missing dataset :x)))))
  (testing "what a dataset can't hold, and two columns of one name"
    (is (thrown-with-msg? ExceptionInfo #"\"x\", which holds calendar intervals"
                          (g/to-tmd (g/sql @tr/spark "SELECT MAKE_INTERVAL(1, 2) x"))))
    (is (thrown-with-msg? ExceptionInfo #"\"x\", which holds calendar intervals"
                          (g/to-tmd (g/sql @tr/spark "SELECT ARRAY(MAKE_INTERVAL(1, 2)) x"))))
    (is (thrown-with-msg? ExceptionInfo #"two columns are named \"a\""
                          (g/to-tmd (g/sql @tr/spark "SELECT 1 a, 2 a"))))))

(deftest ^:classic to-tmd-vectors-test
  (testing "MLlib's vectors, as g/collect gives them"
    (let [dataset (g/to-tmd (g/table->dataset @tr/spark
                                              [[(g/dense 1.0 2.0)] [(g/sparse 3 [1] [5.0])]]
                                              [:v]))]
      (is (= [[1.0 2.0] {:size 3 :indices [1] :values [5.0]}] (vec (dataset :v)))))))

(defn- without-task-error-logs
  "Calls `f` with Spark's executor and scheduler logs off, for a check whose
  Spark job fails on purpose, which they'd log with a stack trace. Over Spark
  Connect, the server does that logging, so a client without log4j2's core
  just calls `f`."
  [f]
  (if-not (class-named "org.apache.logging.log4j.core.config.Configurator")
    (f)
    (let [call      #(Reflector/invokeStaticMethod ^String %1 ^String %2 (object-array %&))
          set-level #(call "org.apache.logging.log4j.core.config.Configurator" "setLevel" %1 %2)
          loggers   ["org.apache.spark.executor.Executor" "org.apache.spark.scheduler.TaskSetManager"]
          before    (mapv #(Reflector/invokeInstanceMethod
                            (call "org.apache.logging.log4j.LogManager" "getLogger" %)
                            "getLevel" (object-array 0))
                          loggers)]
      (try
        (doseq [logger loggers]
          (set-level logger (call "org.apache.logging.log4j.Level" "toLevel" "OFF")))
        (f)
        (finally
          (doseq [[logger level] (map vector loggers before)]
            (set-level logger level)))))))

(def ^:private three-partitions
  (delay (g/sql @tr/spark "SELECT id FROM RANGE(0, 10, 1, 3)")))

(deftest stream-test
  (testing "a dataset per batch that has rows"
    (is (= [3 3 4] (into [] (map ds/row-count) (g/stream @three-partitions))))
    (is (= [3] (into [] (map ds/row-count) (g/stream (g/filter @three-partitions (g/< :id 3))))))
    (is (= [] (into [] (g/stream (g/limit @three-partitions 0))))))
  (testing "with to-tmd's columns and options"
    (is (= [["id"]] (into [] (map ds/column-names) (g/stream (g/range 0 2 1 1) {:key-fn identity})))))
  (testing "transduce, stopping early"
    (is (= [3] (into [] (comp (take 1) (map ds/row-count)) (g/stream @three-partitions)))))
  (testing "Spark's error when a batch fails"
    (without-task-error-logs
     #(is (thrown-with-msg? Exception #"boom"
                            (into [] (g/stream (g/sql @tr/spark (str "SELECT IF(id < 2, id, raise_error('boom')) id "
                                                                     "FROM RANGE(0, 4, 1, 4)"))))))))
  (testing "a reducing function's error, after which the session still works"
    (is (thrown-with-msg? ExceptionInfo #"stop"
                          (reduce (fn [_ _] (throw (ex-info "stop" {}))) nil (g/stream @three-partitions))))
    (is (= 10 (g/count @three-partitions))))
  (testing "as an Iterable, closed with with-open"
    (with-open [batches (g/stream @three-partitions)]
      (is (= 3 (ds/row-count (first batches))))
      (is (= 3 (count (seq batches)))))))

(deftest ^:classic stream-early-stop-test
  (testing "on classic Spark, a partition runs only when a reduce gets to it"
    (is (= [1] (into [] (comp (take 1) (map ds/row-count))
                     (g/stream (g/sql @tr/spark (str "SELECT IF(id < 1, id, raise_error('boom')) id "
                                                     "FROM RANGE(0, 4, 1, 4)"))))))))

(deftest to-tensors-test
  (let [df (g/sql @tr/spark "SELECT id, id * 0.5D half, ARRAY(id, id + 1) pair FROM RANGE(0, 4, 1, 2)")]
    (testing "a numeric column as a 1-D tensor, and arrays as a 2-D one"
      (let [tensors (g/to-tensors df)]
        (is (= #{:id :half :pair} (set (keys tensors))))
        (is (= [[4] :int64 [0 1 2 3]]
               ((juxt dtype/shape dtype/elemwise-datatype dtt/->jvm) (:id tensors))))
        (is (= [0.0 0.5 1.0 1.5] (dtt/->jvm (:half tensors))))
        (is (= [[4 2] :int64 [[0 1] [1 2] [2 3] [3 4]]]
               ((juxt dtype/shape dtype/elemwise-datatype dtt/->jvm) (:pair tensors))))))
    (testing "the columns it's given, under :key-fn's names"
      (is (= ["half"] (keys (g/to-tensors df {:columns [:half] :key-fn identity})))))
    (testing "a map per batch with stream-tensors"
      (is (= [[2] [2]] (into [] (map (comp dtype/shape :id)) (g/stream-tensors df {:columns [:id]}))))))
  (testing "what it can't take, by name"
    (doseq [[reason sql] [["which has nulls" "SELECT IF(id = 1, NULL, id) x FROM RANGE(3)"]
                          ["which has nulls" "SELECT ARRAY(1.0D, NULL) x"]
                          ["whose type is string" "SELECT 'a' x"]
                          ["whose type is boolean" "SELECT true x"]
                          ["whose type is array<string>" "SELECT ARRAY('a') x"]
                          ;; Within one batch, and across batches.
                          ["whose arrays aren't all one length"
                           "SELECT IF(id = 1, ARRAY(1.0D), ARRAY(1.0D, 2.0D)) x FROM RANGE(0, 3, 1, 1)"]
                          ["whose arrays aren't all one length"
                           "SELECT IF(id = 1, ARRAY(1.0D), ARRAY(1.0D, 2.0D)) x FROM RANGE(0, 3, 1, 3)"]]]
      (is (thrown-with-msg? ExceptionInfo (re-pattern (str "the column \"x\", " reason))
                            (g/to-tensors (g/sql @tr/spark sql))))))
  (testing "nor an empty result, nor two columns of one name"
    (is (thrown-with-msg? ExceptionInfo #"at least one row"
                          (g/to-tensors (g/sql @tr/spark "SELECT id FROM RANGE(0)"))))
    (is (thrown-with-msg? ExceptionInfo #"two columns are named \"a\""
                          (g/to-tensors (g/sql @tr/spark "SELECT 1 a, 2 a"))))))

(deftest ^:classic to-tensors-vectors-test
  (let [df (g/table->dataset @tr/spark
                             [[(g/dense 1.0 2.0) (g/dense 1.0 2.0)]
                              [(g/dense 3.0 4.0) (g/sparse 2 [1] [5.0])]]
                             [:dense :mixed])]
    (testing "dense MLlib vectors as a 2-D tensor"
      (is (= [[1.0 2.0] [3.0 4.0]] (dtt/->jvm (:dense (g/to-tensors df {:columns [:dense]}))))))
    (testing "but not sparse ones"
      (is (thrown-with-msg? ExceptionInfo #"\"mixed\", which has sparse vectors"
                            (g/to-tensors df {:columns [:mixed]}))))))

(deftest create-dataframe-test
  (testing "a round trip through to-tmd keeps the types and values"
    (let [df   (g/sql @tr/spark types-sql)
          back (g/create-dataframe @tr/spark (g/to-tmd df))]
      (is (= (dissoc (simple-types df) :m :dec)
             (dissoc (simple-types back) :m :dec)))
      (is (= "decimal(38,18)" (:dec (simple-types back))))
      (is (= (g/collect-vals (g/drop df :m)) (g/collect-vals (g/drop back :m))))
      (testing "except that a map becomes a struct, as in records->dataset"
        (is (= [{:k 0} {:k 1} {:k 2}] (g/collect-col back :m))))))
  (testing "a dataset of plain Clojure data, on the default session"
    (let [back (g/create-dataframe (ds/->dataset {:n [1 2 nil] :s ["x" nil "z"] :k [:p :q :r]}))]
      (is (= {:n "LongType" :s "StringType" :k "StringType"} (g/dtypes back)))
      (is (= [{:n 1 :s "x" :k "p"} {:n 2 :s nil :k "q"} {:n nil :s "z" :k "r"}]
             (g/collect (g/order-by back :k))))))
  (testing "VARIANT on Spark 4"
    (when (spark-4?)
      (let [df (g/sql @tr/spark "SELECT PARSE_JSON('{\"a\": 1}') v")]
        (is (= {:v "VariantType"} (g/dtypes (g/create-dataframe @tr/spark (g/to-tmd df)))))
        (is (= ["{\"a\":1}"]
               (map str (g/collect-col (g/create-dataframe @tr/spark (g/to-tmd df)) :v)))))))
  (testing "an empty dataset"
    (is (zero? (g/count (g/create-dataframe @tr/spark (g/to-tmd (g/limit (g/range 3) 0)))))))
  (testing "and what it takes"
    (is (thrown-with-msg? ExceptionInfo #"tech.ml.dataset dataset"
                          (g/create-dataframe @tr/spark {:a [1]})))))
