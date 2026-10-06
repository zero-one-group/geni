(ns zero-one.geni.results-test
  "The result functions that need neither tech.ml.dataset nor Apache Arrow:
  to-arrow, glimpse and to-html, and what the rest say when what they need
  is missing. test-tmd/ has the others."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.core.results :as results]
   [zero-one.geni.test-resources :as tr]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang ExceptionInfo)
   (java.nio ByteBuffer ByteOrder)
   (java.util Iterator TimeZone)))

(defn- continuation-marker?
  "Whether an Arrow IPC stream starts as one does: with the continuation
  marker, 0xFFFFFFFF, before its schema message."
  [^bytes stream]
  (= -1 (.getInt (.order (ByteBuffer/wrap stream) ByteOrder/LITTLE_ENDIAN) 0)))

(deftest to-arrow-test
  (testing "one complete Arrow IPC stream per batch"
    (let [streams (g/to-arrow (g/sql @tr/spark "SELECT id, CAST(id AS STRING) s FROM RANGE(0, 6, 1, 3)"))]
      (is (vector? streams))
      (is (= 3 (count streams)))
      (is (every? bytes? streams))
      (is (every? continuation-marker? streams))))
  (testing "and one stream with no rows for an empty result"
    (let [streams (g/to-arrow (g/sql @tr/spark "SELECT id FROM RANGE(0)"))]
      (is (= 1 (count streams)))
      (is (continuation-marker? (first streams))))))

(deftest ^:classic to-arrow-read-back-test
  (testing "the streams read back with Arrow, which classic Spark brings"
    (let [read-rows (requiring-resolve 'zero-one.geni.arrow-rows/read-rows)
          streams   (g/to-arrow (g/sql @tr/spark (str "SELECT id, CONCAT('s', id) s, "
                                                      "IF(id = 1, NULL, id * 1.5D) d "
                                                      "FROM RANGE(0, 3, 1, 2)")))]
      (is (= [[0 "s0" 0.0] [1 "s1" nil] [2 "s2" 3.0]]
             (mapcat read-rows streams))))))

(deftest ^:classic to-arrow-time-zone-test
  (testing "a TIMESTAMP's time zone is the session's, which is the JVM's when it isn't set"
    (when-not (.contains (.conf @tr/spark) "spark.sql.session.timeZone")
      (let [time-zones (requiring-resolve 'zero-one.geni.arrow-rows/time-zones)
            before     (TimeZone/getDefault)]
        (try
          (TimeZone/setDefault (TimeZone/getTimeZone "Asia/Jakarta"))
          (is (= ["Asia/Jakarta"]
                 (time-zones (first (g/to-arrow (g/sql @tr/spark "SELECT TIMESTAMP'2026-01-01 12:00:00' ts"))))))
          (finally
            (TimeZone/setDefault before)))))))

(deftest observed-to-arrow-test
  (testing "an observation gets its metrics from to-arrow, as from collect"
    (let [observation (g/observation)
          df          (g/observe (g/range 0 5 1 2) observation {:n (g/count "*")})]
      (g/to-arrow df)
      (is (= {:n 5} (tr/observed-within observation 10000))))))

(deftest twice-named-fields-test
  (testing "a struct with two fields of one name throws, naming its column, before a job runs"
    (let [df (g/sql @tr/spark "SELECT 1 id, ARRAY(NAMED_STRUCT('a', 1, 'a', 2)) xs")]
      (is (thrown-with-msg? ExceptionInfo
                            #"to-tmd can't convert the column \"xs\", which has a struct with two fields named \"a\""
                            (g/to-tmd df)))
      (is (thrown-with-msg? ExceptionInfo #"stream can't convert the column \"xs\""
                            (g/stream df))))))

(def ^:private glimpse-df
  (delay (g/sql @tr/spark (str "SELECT id, CONCAT('s', id) name, "
                               "IF(id = 1, NULL, id * 1.5D) score, ARRAY(id, id) pair, "
                               "NAMED_STRUCT('x', id) st, DATE'2026-01-01' day "
                               "FROM RANGE(12)"))))

(deftest glimpse-test
  (testing "one line per column, with its type and its first values"
    (is (= (str "Rows: at least 3\n"
                "Columns: 6\n"
                "$ id    <bigint> 0, 1, 2\n"
                "$ name  <string> \"s0\", \"s1\", \"s2\"\n"
                "$ score <double> 0.0, nil, 3.0\n"
                "$ pair  <array<bigint>> [0 0], [1 1], [2 2]\n"
                "$ st    <struct<x:bigint>> {:x 0}, {:x 1}, {:x 2}\n"
                "$ day   <date> 2026-01-01, 2026-01-01, 2026-01-01\n")
           (with-out-str (g/glimpse @glimpse-df {:num-rows 3})))))
  (testing "10 values by default, with lines cut at 80 characters"
    (let [lines (string/split-lines (with-out-str (g/glimpse @glimpse-df)))]
      (is (= "Rows: at least 10" (first lines)))
      (is (= "$ id    <bigint> 0, 1, 2, 3, 4, 5, 6, 7, 8, 9" (nth lines 2)))
      (is (= "$ name  <string> \"s0\", \"s1\", \"s2\", \"s3\", \"s4\", \"s5\", \"s6\", \"s7\", \"s8\", \"s9\""
             (nth lines 3)))
      (is (= "$ st    <struct<x:bigint>> {:x 0}, {:x 1}, {:x 2}, {:x 3}, {:x 4}, {:x 5}, {:x …"
             (nth lines 6)))))
  (testing "nothing cut with ##Inf"
    (is (every? #(not (string/ends-with? % "…"))
                (string/split-lines (with-out-str (g/glimpse @glimpse-df {:width ##Inf}))))))
  (testing "an exact count when the sample comes back short, or when asked"
    (is (string/starts-with? (with-out-str (g/glimpse (g/limit @glimpse-df 2))) "Rows: 2\n"))
    (is (string/starts-with? (with-out-str (g/glimpse @glimpse-df {:num-rows 2 :count true}))
                             "Rows: 12\n")))
  (testing "options that it can't take"
    (is (thrown-with-msg? IllegalArgumentException #"g/dtypes"
                          (g/glimpse @glimpse-df {:num-rows 0})))
    (is (thrown-with-msg? IllegalArgumentException #"##Inf"
                          (g/glimpse @glimpse-df {:width -1})))))

(deftest to-html-test
  (let [df (g/sql @tr/spark "SELECT '<b>&</b>' tag, id FROM RANGE(25)")]
    (testing "Spark's own HTML table, with the cells escaped"
      (let [html (g/to-html df {:num-rows 2})]
        (is (string/starts-with? html "<table border='1'>\n<tr><th>tag</th><th>id</th></tr>\n"))
        (is (string/includes? html "<td>&lt;b&gt;&amp;&lt;/b&gt;</td><td>1</td>"))
        (is (not (string/includes? html "<td>2</td>")))
        (is (string/includes? html "only showing top 2 rows"))))
    (testing "20 rows by default, with the cells cut at 20 characters"
      (is (string/includes? (g/to-html df) "only showing top 20 rows"))
      (is (string/includes? (g/to-html (g/sql @tr/spark "SELECT repeat('x', 30) s"))
                            "<td>xxxxxxxxxxxxxxxxx...</td>"))
      (is (string/includes? (g/to-html (g/sql @tr/spark "SELECT repeat('x', 30) s")
                                       {:truncate false})
                            (str "<td>" (apply str (repeat 30 "x")) "</td>"))))
    (testing "options that it can't take"
      (is (thrown? IllegalArgumentException (g/to-html df {:num-rows -1}))))))

(deftest what-the-rest-need-test
  (let [df (g/range 3)]
    (testing "decoding names Apache Arrow when it's missing, as with a Spark Connect client"
      (when-not (class-named "org.apache.arrow.vector.VectorSchemaRoot")
        (is (thrown-with-msg? ExceptionInfo #"arrow-vector" (g/to-tmd df)))
        (is (thrown-with-msg? ExceptionInfo #"arrow-vector" (g/to-tensors df)))
        (is (thrown-with-msg? ExceptionInfo #"arrow-vector" (reduce conj [] (g/stream df))))))
    (testing "and tech.ml.dataset when that is"
      (when (and (class-named "org.apache.arrow.vector.VectorSchemaRoot")
                 (not (io/resource "tech/v3/dataset.clj")))
        (is (thrown-with-msg? ExceptionInfo #"techascent/tech.ml.dataset" (g/to-tmd df)))
        (is (thrown-with-msg? ExceptionInfo #"techascent/tech.ml.dataset" (g/to-tensors df)))))
    (testing "create-dataframe says what it takes after a session"
      (is (thrown-with-msg? ExceptionInfo #"tech.ml.dataset dataset"
                            (g/create-dataframe @tr/spark [{:a 1}]))))))

(defn- counting-source
  "A stand-in for open-streams: an iterator over 100 streams, here numbers,
  that counts how many are read, and how many runs are closed."
  [reads closed]
  (fn [_dataframe _mode]
    (let [^Iterator streams (.iterator ^Iterable (range 100))]
      {:iterator (reify Iterator
                   (hasNext [_] (.hasNext streams))
                   (next [_] (swap! reads inc) (.next streams)))
       :close    #(swap! closed inc)})))

(deftest batches-read-test
  ;; The reducible that g/stream and g/stream-tensors return, over a source
  ;; that counts its reads, with each stream decoded as one batch.
  (let [reads   (atom 0)
        closed  (atom 0)
        batches #(#'results/batches nil "stream" :lazy vector)]
    (with-redefs [results/open-streams     (counting-source reads closed)
                  results/as-sql-execution (fn [_df _fn-name f] (f))]
      (testing "first reads one batch, where Clojure's seq of an Iterable reads 32 ahead"
        (with-open [b (batches)]
          (is (= 0 (first b)))
          (is (= 1 @reads)))
        (is (= 1 @closed)))
      (testing "a seq reads as far as it's realised"
        (reset! reads 0)
        (with-open [b (batches)]
          (is (= [0 1 2] (take 3 b)))
          (is (= 3 @reads))))
      (testing "a reduce reads until it stops, and closes the run"
        (reset! reads 0)
        (reset! closed 0)
        (is (= [0 1 2] (into [] (take 3) (batches))))
        (is (= [3 1] [@reads @closed])))
      (testing "and to the end"
        (reset! reads 0)
        (is (= 100 (count (into [] (batches)))))
        (is (= 100 @reads)))
      (testing "a reduce without an init does as reduce does with a collection, and closes the run"
        (reset! reads 0)
        (reset! closed 0)
        (is (= 4950 (reduce + (batches))))
        (is (= :stop (reduce (fn [_ _] (reduced :stop)) (batches))))
        (is (= [102 2] [@reads @closed]))
        (is (= 0 (reduce + (#'results/batches nil "stream" :lazy (constantly [])))))))))
