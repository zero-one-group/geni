(ns zero-one.geni.results-tmd-test
  "g/to-tmd, g/stream, g/to-tensors, g/stream-tensors and g/create-dataframe
  with tech.ml.dataset, on classic Spark and over Spark Connect."
  (:require
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.column :as ds-col]
   [tech.v3.datatype :as dtype]
   [tech.v3.tensor :as dtt]
   [zero-one.geni.core :as g]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.test-resources :as tr])
  (:import
   (clojure.lang ExceptionInfo)
   (java.time Duration Instant LocalDate LocalDateTime LocalTime Period)
   (java.time.temporal ChronoUnit)))

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
                :wait    :duration
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
    (is (= ["id"] (ds/column-names (g/to-tmd (g/range 2) {:key-fn identity})))))
  (testing "each column's Spark type, as DDL, in its metadata"
    (let [dataset (g/to-tmd (g/sql @tr/spark "SELECT CAST(1 AS DECIMAL(10, 2)) d, MAP('k', 1) m"))]
      (is (= ["DECIMAL(10,2)" "MAP<STRING, INT>"]
             (map #(:zero-one.geni/spark-type (meta (dataset %))) [:d :m]))))))

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
  (testing "day-time intervals as Durations, past where nanoseconds in a long end"
    (let [dataset (g/to-tmd (g/sql @tr/spark (str "SELECT INTERVAL '200000' DAY d, "
                                                  "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND top, "
                                                  "-INTERVAL '106751991 04:00:54.775807' DAY TO SECOND bottom")))]
      (is (= [:duration (Duration/ofDays 200000)] [(:datatype (meta (dataset :d))) (first (dataset :d))]))
      (is (= [(Duration/of Long/MAX_VALUE ChronoUnit/MICROS) (Duration/of (- Long/MAX_VALUE) ChronoUnit/MICROS)]
             [(first (dataset :top)) (first (dataset :bottom))]))))
  (testing "and of one datatype across batches"
    (let [df (g/sql @tr/spark "SELECT make_dt_interval(id * 100000) d FROM RANGE(0, 4, 1, 4)")]
      (is (= (map #(Duration/ofDays (* 100000 %)) (range 4)) (vec ((g/to-tmd df) :d))))
      (is (= [:duration :duration :duration :duration]
             (into [] (map #(:datatype (meta (% :d)))) (g/stream df))))))
  (testing "inside an array, up to where Arrow's Java reader gives them exactly"
    (is (= [[(Duration/ofDays 100000) (Duration/ofDays -100000)]]
           (vec ((g/to-tmd (g/sql @tr/spark "SELECT ARRAY(INTERVAL '100000' DAY, -INTERVAL '100000' DAY) a")) :a))))
    (is (thrown-with-msg? ExceptionInfo #"column \"a\" has a day-time interval of more than 106,751 days"
                          (g/to-tmd (g/sql @tr/spark "SELECT ARRAY(INTERVAL '200000' DAY) a")))))
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
                          (g/to-tmd (g/sql @tr/spark "SELECT 1 a, 2 a")))))
  (testing "nor two columns that :key-fn names alike, before a job runs"
    (is (thrown-with-msg? ExceptionInfo #"to-tmd's :key-fn gives the columns \"a\" and \"A\" one name, :a"
                          (g/to-tmd (g/sql @tr/spark (str "SELECT IF(id < 0, 1, raise_error('boom')) a, "
                                                          "id A FROM RANGE(1)"))
                                    {:key-fn (comp keyword string/lower-case)}))))
  (testing "no columns and no rows, as an empty dataset"
    (is (= [0 0] ((juxt ds/row-count ds/column-count)
                  (g/to-tmd (g/select (g/limit (g/range 3) 0) []))))))
  (testing "but no columns and some rows throws, since a dataset counts rows by its columns"
    (is (thrown-with-msg? ExceptionInfo #"to-tmd can't give rows without columns"
                          (g/to-tmd (g/select (g/range 3) []))))))

(deftest ^:classic to-tmd-vectors-test
  (testing "MLlib's vectors, as g/collect gives them"
    (let [dataset (g/to-tmd (g/table->dataset @tr/spark
                                              [[(g/dense 1.0 2.0)] [(g/sparse 3 [1] [5.0])]]
                                              [:v]))]
      (is (= [[1.0 2.0] {:size 3 :indices [1] :values [5.0]}] (vec (dataset :v)))))))

(def ^:private three-partitions
  (delay (g/sql @tr/spark "SELECT id FROM RANGE(0, 10, 1, 3)")))

(deftest stream-test
  (testing "a dataset per batch that has rows"
    (is (= [3 3 4] (into [] (map ds/row-count) (g/stream @three-partitions))))
    (is (= [3] (into [] (map ds/row-count) (g/stream (g/filter @three-partitions (g/< :id 3))))))
    (is (= [] (into [] (g/stream (g/limit @three-partitions 0))))))
  (testing "with to-tmd's columns and options"
    (is (= [["id"]] (into [] (map ds/column-names) (g/stream (g/range 0 2 1 1) {:key-fn identity}))))
    (is (thrown-with-msg? ExceptionInfo #"stream's :key-fn gives the columns \"a\" and \"A\" one name"
                          (g/stream (g/sql @tr/spark "SELECT 1 a, 2 A") {:key-fn (comp keyword string/lower-case)}))))
  (testing "and its rule for rows without columns"
    (is (= [] (into [] (g/stream (g/select (g/limit @three-partitions 0) [])))))
    (is (thrown-with-msg? ExceptionInfo #"stream can't give rows without columns"
                          (into [] (g/stream (g/select @three-partitions []))))))
  (testing "transduce, stopping early"
    (is (= [3] (into [] (comp (take 1) (map ds/row-count)) (g/stream @three-partitions)))))
  (testing "Spark's error when a batch fails"
    (tr/without-task-error-logs
     #(is (thrown-with-msg? Exception #"boom"
                            (into [] (g/stream (g/sql @tr/spark (str "SELECT IF(id < 2, id, raise_error('boom')) id "
                                                                     "FROM RANGE(0, 4, 1, 4)"))))))))
  (testing "a reducing function's error, after which the session still works"
    (is (thrown-with-msg? ExceptionInfo #"stop"
                          (reduce (fn [_ _] (throw (ex-info "stop" {}))) nil (g/stream @three-partitions))))
    (is (= 10 (g/count @three-partitions))))
  (testing "as a seq, closed with with-open"
    (with-open [batches (g/stream @three-partitions)]
      (is (= 3 (ds/row-count (first batches))))
      (is (= 3 (count (seq batches)))))))

(deftest ^:classic stream-early-stop-test
  (let [one-good-partition #(g/sql @tr/spark (str "SELECT IF(id < 1, id, raise_error('boom')) id "
                                                  "FROM RANGE(0, " % ", 1, " % ")"))]
    (testing "on classic Spark, a partition runs only when a reduce gets to it"
      (is (= [1] (into [] (comp (take 1) (map ds/row-count)) (g/stream (one-good-partition 4))))))
    (testing "and when a seq does, one batch at a time, not 32"
      (with-open [batches (g/stream (one-good-partition 40))]
        (is (= 1 (ds/row-count (first batches))))))))

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
      (is (= [[2] [2]] (into [] (map (comp dtype/shape :id)) (g/stream-tensors df {:columns [:id]})))))
    (testing "every integer and floating-point type"
      (is (= {:b :int8 :s :int16 :i :int32 :l :int64 :f :float32 :d :float64}
             (update-vals (g/to-tensors (g/sql @tr/spark (str "SELECT CAST(1 AS TINYINT) b, CAST(1 AS SMALLINT) s, "
                                                              "1 i, 1L l, CAST(1 AS FLOAT) f, 1.0D d")))
                          dtype/elemwise-datatype)))))
  (testing "what it can't take, by name"
    (doseq [[reason sql] [["which has nulls" "SELECT IF(id = 1, NULL, id) x FROM RANGE(3)"]
                          ["which has nulls" "SELECT ARRAY(1.0D, NULL) x"]
                          ["whose type is string" "SELECT 'a' x"]
                          ["whose type is boolean" "SELECT true x"]
                          ["whose type is decimal\\(2,1\\)" "SELECT 1.5BD x"]
                          ["whose type is array<string>" "SELECT ARRAY('a') x"]
                          ;; Within one batch, and across batches.
                          ["whose arrays aren't all one length"
                           "SELECT IF(id = 1, ARRAY(1.0D), ARRAY(1.0D, 2.0D)) x FROM RANGE(0, 3, 1, 1)"]
                          ["whose arrays aren't all one length"
                           "SELECT IF(id = 1, ARRAY(1.0D), ARRAY(1.0D, 2.0D)) x FROM RANGE(0, 3, 1, 3)"]]]
      (is (thrown-with-msg? ExceptionInfo (re-pattern (str "the column \"x\", " reason))
                            (g/to-tensors (g/sql @tr/spark sql))))))
  (testing "nor an empty result, rows without columns, or two columns of one name"
    (is (thrown-with-msg? ExceptionInfo #"at least one row"
                          (g/to-tensors (g/sql @tr/spark "SELECT id FROM RANGE(0)"))))
    (is (thrown-with-msg? ExceptionInfo #"to-tensors can't give rows without columns"
                          (g/to-tensors (g/select (g/range 3) []))))
    (is (thrown-with-msg? ExceptionInfo #"stream-tensors can't give rows without columns"
                          (into [] (g/stream-tensors (g/select (g/range 3) [])))))
    (is (thrown-with-msg? ExceptionInfo #"two columns are named \"a\""
                          (g/to-tensors (g/sql @tr/spark "SELECT 1 a, 2 a"))))
    (is (thrown-with-msg? ExceptionInfo #"to-tensors's :key-fn gives the columns \"a\" and \"A\" one name"
                          (g/to-tensors (g/sql @tr/spark "SELECT 1 a, 2 A") {:key-fn (comp keyword string/lower-case)})))
    (is (thrown-with-msg? ExceptionInfo #"stream-tensors's :key-fn gives the columns \"a\" and \"A\" one name"
                          (g/stream-tensors (g/sql @tr/spark "SELECT 1 a, 2 A") {:key-fn (comp keyword string/lower-case)})))))

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
  (testing "a round trip through to-tmd keeps the types, from each column's metadata, and the values"
    (let [df   (g/sql @tr/spark types-sql)
          back (g/create-dataframe @tr/spark (g/to-tmd df))]
      (is (= (simple-types df) (simple-types back)))
      (is (= (g/collect-vals df) (g/collect-vals back)))))
  (testing "decimals of any precision and scale, and maps with any keys"
    (let [df   (g/sql @tr/spark (str "SELECT CAST('123456789012345678901234567890' AS DECIMAL(38, 0)) big, "
                                     "CAST('-0.123456789012345678901234567890' AS DECIMAL(38, 30)) fine, "
                                     "CAST(NULL AS DECIMAL(5, 2)) none, MAP(1, 'a') m"))
          back (g/create-dataframe @tr/spark (g/to-tmd df))]
      (is (= {:big "decimal(38,0)" :fine "decimal(38,30)" :none "decimal(5,2)" :m "map<int,string>"}
             (simple-types back)))
      (is (= (g/collect-vals df) (g/collect-vals back)))))
  (testing "intervals of any length, with their fields"
    (let [df   (g/sql @tr/spark (str "SELECT INTERVAL '200000' DAY d, INTERVAL '1 02:03' DAY TO MINUTE m, "
                                     "INTERVAL '3' YEAR y"))
          back (g/create-dataframe @tr/spark (g/to-tmd df))]
      ;; Over Spark Connect, createDataFrame gives an interval all its fields.
      (when (classic?)
        (is (= (simple-types df) (simple-types back))))
      (is (= [(Duration/ofDays 200000) (Duration/parse "PT26H3M") (Period/ofYears 3)]
             (map #(first ((g/to-tmd back) %)) [:d :m :y])))))
  (testing "the metadata's type only while the column has the datatype that to-tmd gave it"
    (let [dataset (ds/new-dataset [{:tech.v3.dataset/name     :s
                                    :tech.v3.dataset/data     ["a" "b"]
                                    :tech.v3.dataset/metadata {:zero-one.geni/spark-type "BIGINT"}}])]
      (is (= {:s "string"} (simple-types (g/create-dataframe @tr/spark dataset))))))
  (testing "and a value that the metadata's type can't hold throws, naming the column"
    (let [dataset (ds/concat (g/to-tmd (g/sql @tr/spark "SELECT CAST(1.5 AS DECIMAL(10, 2)) d"))
                             (ds/->dataset {:d [1.125M]}))]
      (is (thrown-with-msg? ExceptionInfo #"column \"d\" has the value 1.125M, which DECIMAL\(10,2\) can't hold"
                            (g/create-dataframe @tr/spark dataset)))))
  (testing "decimals of a dataset made by hand, with room for all of a column's values"
    (let [back (g/create-dataframe @tr/spark
                                   (ds/->dataset {:big   [123456789012345678901234567890M nil -1.5M]
                                                  :fine  [0.123456789012345678901234567890M -2M nil]
                                                  :plain [1.5M 2.25M nil]}))]
      (is (= {:big "decimal(38,8)" :fine "decimal(38,30)" :plain "decimal(38,18)"} (simple-types back)))
      (is (= [[123456789012345678901234567890M 0.123456789012345678901234567890M 1.5M]
              [nil -2M 2.25M]
              [-1.5M nil nil]]
             (g/collect-vals back))))
    (is (thrown-with-msg? ExceptionInfo
                          #"column \"x\" has numbers with up to 12 digits before the point and 30 after it"
                          (g/create-dataframe @tr/spark (ds/->dataset {:x [0.123456789012345678901234567890M
                                                                           123456789012M]})))))
  (testing ":schema gives columns their types"
    (let [dataset (ds/->dataset {:price [1.5M 2.25M nil] :n [1 2 3]})]
      (is (= {:price "decimal(5,2)" :n "int"}
             (simple-types (g/create-dataframe @tr/spark dataset {:schema {:price "DECIMAL(5, 2)" "n" :int}}))))
      (is (= [[1.5M 1] [2.25M 2] [nil 3]]
             (g/collect-vals (g/create-dataframe dataset {:schema {:price (g/parse-ddl "DECIMAL(5, 2)")}}))))
      (testing "and values that they can't hold throw, naming the column"
        (is (thrown-with-msg? ExceptionInfo #"column \"price\" has the value 2.25M, which DECIMAL\(5,1\) can't hold"
                              (g/create-dataframe @tr/spark dataset {:schema {:price "DECIMAL(5, 1)"}})))
        (is (thrown-with-msg? ExceptionInfo #"column \"n\" has the value 300, which TINYINT can't hold"
                              (g/create-dataframe @tr/spark (ds/->dataset {:n [1 300]}) {:schema {:n :byte}})))
        (is (thrown-with-msg? ExceptionInfo #"column \"w\" has the value .*PT25H.*, which INTERVAL DAY can't hold"
                              (g/create-dataframe @tr/spark (ds/->dataset {:w [(Duration/ofHours 25)]})
                                                  {:schema {:w "INTERVAL DAY"}}))))
      (testing "and only the dataset's columns"
        (is (thrown-with-msg? ExceptionInfo #"doesn't have: \[\"nope\"\]"
                              (g/create-dataframe @tr/spark dataset {:schema {:nope :int}}))))))
  (testing "a dataset of plain Clojure data, on the default session"
    (let [back (g/create-dataframe (ds/->dataset {:n [1 2 nil] :s ["x" nil "z"] :k [:p :q :r]}))]
      (is (= {:n "LongType" :s "StringType" :k "StringType"} (g/dtypes back)))
      (is (= [{:n 1 :s "x" :k "p"} {:n 2 :s nil :k "q"} {:n nil :s "z" :k "r"}]
             (g/collect (g/order-by back :k))))))
  (testing "a map of a dataset made by hand as a struct, as in records->dataset"
    (is (= [{:a 1 :b nil} {:a nil :b "x"}]
           (g/collect-col (g/create-dataframe @tr/spark (ds/->dataset {:m [{:a 1} {:b "x"}]})) :m))))
  (testing "VARIANT on Spark 4"
    (when (spark-4?)
      (let [df (g/sql @tr/spark "SELECT PARSE_JSON('{\"a\": 1}') v")]
        (is (= {:v "VariantType"} (g/dtypes (g/create-dataframe @tr/spark (g/to-tmd df)))))
        (is (= ["{\"a\":1}"]
               (map str (g/collect-col (g/create-dataframe @tr/spark (g/to-tmd df)) :v)))))))
  (testing "an empty dataset, and one without columns, which has no rows"
    (is (zero? (g/count (g/create-dataframe @tr/spark (g/to-tmd (g/limit (g/range 3) 0))))))
    (let [back (g/create-dataframe @tr/spark (ds/new-dataset []))]
      (is (= [true 0] [(empty? (g/column-names back)) (g/count back)]))))
  (testing "and what it takes"
    (is (thrown-with-msg? ExceptionInfo #"tech.ml.dataset dataset"
                          (g/create-dataframe @tr/spark {:a [1]})))
    (is (thrown-with-msg? ExceptionInfo #"takes a map of options after a dataset"
                          (g/create-dataframe (ds/->dataset {:a [1]}) [:a])))))
