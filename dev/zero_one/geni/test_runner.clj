(ns zero-one.geni.test-runner
  "Runs the clojure.test suite, one namespace at a time, with one line per
  namespace and one line per failure. Full reports go to target/test.log.
  See CONTRIBUTING.md."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :as t]))

(def ^:private compiled-java
  "target/classes/zero_one/geni/rdd/function/Fn1.class")

(def ^:private prepped-spark
  "What `clojure -T:build prep` compiled the test namespaces against."
  "target/test-classes/spark.edn")

(def ^:private default-report t/report)

(defn- running-spark
  "The Spark and Scala versions on the classpath, without starting Spark."
  []
  (let [props (java.util.Properties.)]
    (with-open [in (io/input-stream (io/resource "spark-version-info.properties"))]
      (.load props in))
    {:spark (.getProperty props "version")
     :scala (re-find #"^\d+\.\d+" (scala.util.Properties/versionNumberString))}))

(defn- prep-problem
  "Why the compiled classes don't suit this run, if they don't."
  []
  (let [prepped (io/file prepped-spark)]
    (cond
      (not (.exists (io/file compiled-java)))
      "Compiled Java classes not found. Run `clojure -T:build prep` first."

      (.exists prepped)
      (let [built   (edn/read-string (slurp prepped))
            running (running-spark)]
        (when (not= built running)
          (format (str "The test namespaces were compiled for Spark %s on Scala %s, but this is "
                       "Spark %s on Scala %s. Run `clojure -T:build prep` with the same Spark "
                       "alias, e.g. `clojure -T:build prep :spark :spark-4`.")
                  (:spark built) (:scala built) (:spark running) (:scala running)))))))

(defn- classpath-dirs
  "The project's own directories on the classpath, so that each alias (e.g.
  :cli, :tmd or :xgb) brings its tests along."
  []
  (->> (.split (System/getProperty "java.class.path") java.io.File/pathSeparator)
       (remove #(or (string/starts-with? % "target")
                    (.isAbsolute (io/file %))))
       (filter #(.isDirectory (io/file %)))))

(defn- test-namespaces
  "The test namespaces in `dirs`, as [name file] pairs, sorted by name."
  [dirs]
  (sort-by first
           (for [dir  dirs
                 file (file-seq (io/file dir))
                 :let [path (.getPath ^java.io.File file)]
                 :when (string/ends-with? path "_test.clj")]
             [(-> path
                  (subs (inc (count dir)))
                  (string/replace #"\.clj$" "")
                  (string/replace "/" ".")
                  (string/replace "_" "-")
                  symbol)
              path])))

(defn- ns-file
  "Where a namespace's source is on the classpath, if it's there."
  [ns-sym]
  (io/resource (str (-> (str ns-sym) (string/replace "-" "_") (string/replace "." "/")) ".clj")))

(defn- ns-meta
  "The metadata on the namespace's name in its file, such as ^:classic, read
  without loading the namespace."
  [path]
  (with-open [r (java.io.PushbackReader. (io/reader path))]
    (binding [*read-eval* false]
      (let [form (read r)]
        (when (and (seq? form) (= 'ns (first form)))
          (meta (second form)))))))

(defn- skipped
  "The tests that this run skips, by the metadata that marks them, with why:
  the ^:classic ones over Spark Connect, when Spark's JVM client is on the
  classpath without classic Spark, and the ^:connect ones otherwise, the
  ^:xgb ones without XGBoost4J-Spark 3 on the classpath, and the ^:spark-3
  ones on Spark 4."
  []
  (let [class-named (requiring-resolve 'zero-one.geni.utils/class-named)]
    (cond-> (if ((requiring-resolve 'zero-one.geni.spark/connect-only?))
              {:classic "classic Spark only"}
              {:connect "Spark Connect only"})
      (not (class-named "ml.dmlc.xgboost4j.scala.spark.XGBoostRanker"))
      (assoc :xgb "needs XGBoost4J-Spark 3")

      (string/starts-with? (:spark (running-spark)) "4.")
      (assoc :spark-3 "Spark 3.5 only"))))

(defn- skip-reason
  "Why this run skips what has the metadata `m`, if it does."
  [skipped m]
  (some (fn [[k why]] (when (get m k) why)) skipped))

(defn- shard
  "Every nth namespace, starting from the ith: [i n], counting from 1."
  [namespaces [i n]]
  (if n
    (keep-indexed #(when (= (mod %1 n) (dec i)) %2) namespaces)
    namespaces))

(defn- one-line [x limit]
  (let [s (string/replace (str x) #"\s+" " ")]
    (if (> (count s) limit) (str (subs s 0 limit) "...") s)))

(defn- test-frame
  "Where in a test file an exception came from, if anywhere."
  [^Throwable e]
  (some (fn [^StackTraceElement el]
          (when (some-> (.getFileName el) (string/ends-with? "_test.clj"))
            [(.getFileName el) (.getLineNumber el)]))
        (.getStackTrace e)))

(defn- failure-line [{:keys [type var contexts expected actual] :as m}]
  (let [[file line] (or (when (instance? Throwable actual) (test-frame actual))
                        [(:file m) (:line m)])
        where (str file ":" line " " (some-> var meta :name)
                   (when (seq contexts) (str " > " (string/join " > " contexts))))]
    (if (= type :error)
      (str "ERROR " where "\n        " (one-line (if (instance? Throwable actual)
                                                   (str (.getName (class actual)) ": " (ex-message actual))
                                                   (pr-str actual))
                                                 200))
      (str "FAIL  " where "\n        " (one-line (pr-str actual) 200)
           (when-not (and (seq? actual) (= 'not (first actual)))
             (str " (expected " (one-line (pr-str expected) 100) ")"))))))

(defn- run-namespace
  "Loads one namespace and runs its tests, keeping their output for the log."
  [ns-sym {:keys [include exclude reload skip]} ^java.io.Writer log]
  (let [out      (java.io.StringWriter.)
        failures (atom [])
        timings  (atom {})
        started  (atom {})
        counters (ref t/*initial-report-counters*)
        start    (System/nanoTime)
        load-err (binding [*out* out]
                   (try
                     (if reload (require ns-sym :reload) (require ns-sym))
                     nil
                     (catch Throwable e e)))
        report   (fn [m]
                   (binding [t/*test-out* out] (default-report m))
                   (case (:type m)
                     (:fail :error) (swap! failures conj
                                           (assoc m
                                                  :var (first t/*testing-vars*)
                                                  :contexts (reverse t/*testing-contexts*)))
                     :begin-test-var (swap! started assoc (:var m) (System/nanoTime))
                     :end-test-var (swap! timings assoc (:var m)
                                          (/ (- (System/nanoTime) (@started (:var m))) 1e9))
                     nil))
        vars     (when-not load-err
                   (->> (vals (ns-interns ns-sym))
                        (filter (comp :test meta))
                        (filter #(or (nil? include) (include (meta %))))
                        (remove #(and exclude (exclude (meta %))))
                        (remove #(skip-reason skip (meta %)))
                        (sort-by (comp :line meta))))]
    (when-not load-err
      (binding [*out*                out
                t/*test-out*         out
                t/*report-counters*  counters
                t/report             report]
        (t/test-vars vars)))
    (when load-err
      (binding [*out* out]
        (println "Failed to load" ns-sym)
        ((requiring-resolve 'clojure.stacktrace/print-cause-trace) load-err)))
    (doto log
      (.write (str "\n==== " ns-sym "\n" out))
      (.flush))
    (let [{:keys [pass fail error]} @counters]
      {:ns        ns-sym
       :seconds   (/ (- (System/nanoTime) start) 1e9)
       :load-err  load-err
       :tests     (count vars)
       :checks    (+ pass fail error)
       :failures  @failures
       :timings   @timings})))

(defn- report! [{:keys [ns seconds load-err tests checks failures]}]
  (let [status (cond load-err "LOAD" (seq failures) "FAIL" :else "ok")
        detail (cond
                 load-err      (str "did not load: " (one-line (ex-message (or (ex-cause load-err) load-err)) 120))
                 (zero? tests) "no tests selected"
                 :else         (format "%d tests, %d checks%s" tests checks
                                       (if (seq failures) (str ", " (count failures) " failed") "")))]
    (println (format "%-4s  %-42s %6.1fs  %s" status ns seconds detail))
    (doseq [f failures]
      (println (str "      " (failure-line f))))
    (flush)))

(defn- passed? [{:keys [load-err failures]}]
  (and (nil? load-err) (empty? failures)))

(defn run-tests
  "Runs the tests and returns the results. See `run` for the options."
  [{:keys [dirs only shard-spec slowest] log-path :log
    :or   {slowest 5 log-path "target/test.log"}
    :as   opts}]
  (io/make-parents log-path)
  (with-open [log (io/writer log-path)]
    (let [start      (System/nanoTime)
          skip       (skipped)
          selected   (if (seq only)
                       (map (juxt identity ns-file) only)
                       (shard (test-namespaces (or dirs (classpath-dirs))) shard-spec))
          reason     (fn [[_ path]] (when path (skip-reason skip (ns-meta path))))
          skip-ns?   (comp boolean reason)
          _          (doseq [[ns-sym :as selection] (filter skip-ns? selected)]
                       (println (format "%-4s  %-42s %6s   %s" "skip" ns-sym "" (reason selection))))
          namespaces (map first (remove skip-ns? selected))
          results    (mapv #(doto (run-namespace % (assoc opts :skip skip) log) report!) namespaces)
          failed     (remove passed? results)
          none?      (empty? results)
          seconds    (/ (- (System/nanoTime) start) 1e9)
          checks     (reduce + (map :checks results))]
      (println)
      (cond
        none?
        (println "No test namespaces found in" (pr-str (or dirs (classpath-dirs))))

        (empty? failed)
        (println (format "All %d test namespaces passed: %d checks in %.1fs."
                         (count results) checks seconds))

        :else
        (println (format "%d of %d test namespaces failed (%.1fs). Full reports are in %s."
                         (count failed) (count results) seconds log-path)))
      (when (and (pos? slowest) (not none?))
        (println "Slowest tests:"
                 (->> (mapcat :timings results)
                      (sort-by val >)
                      (take slowest)
                      (map (fn [[v s]] (format "%s %.1fs" (-> v meta :name) s)))
                      (string/join ", "))))
      {:passed? (and (not none?) (empty? failed)) :results results})))

(defn test!
  "Reloads and runs test namespaces from the REPL, e.g.
  (test! 'zero-one.geni.dataset-test). With no args, runs them all."
  [& namespaces]
  (-> (run-tests {:only (seq namespaces) :reload true :slowest 0})
      :passed?))

(defn run
  "Runs the tests from `clojure -X:spark:test`, and exits with a non-zero
  status if anything fails.

  Options:
    :dirs     the directories to scan (default: the project's classpath dirs),
              e.g. [\"target/test-doc-blocks/test\"] for the doc tests
    :only     the namespaces to run, e.g. [zero-one.geni.dataset-test]
    :include  only run tests with this metadata, e.g. :slow
    :exclude  skip tests with this metadata, e.g. :slow
    :shard    run every nth namespace from the ith, e.g. [1 3]
    :slowest  how many of the slowest tests to list (default: 5)
    :log      the file for the full reports (default: target/test.log)

  Over Spark Connect, that is with Spark's JVM client on the classpath in
  place of classic Spark, it skips the namespaces and tests marked ^:classic.
  Otherwise, it skips the ones marked ^:connect. Without XGBoost4J-Spark 3 on
  the classpath, it skips the ones marked ^:xgb, and on Spark 4 the ones
  marked ^:spark-3."
  [{:keys [shard] :as opts}]
  (when-let [problem (prep-problem)]
    (println problem)
    (System/exit 1))
  (let [{:keys [passed?]} (run-tests (assoc opts :shard-spec shard))]
    (shutdown-agents)
    (System/exit (if passed? 0 1))))
