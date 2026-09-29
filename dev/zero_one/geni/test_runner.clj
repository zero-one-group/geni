(ns zero-one.geni.test-runner
  "Runs the Midje suite from the Clojure CLI. See CONTRIBUTING.md."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test]
   [midje.repl]))

(def ^:private compiled-java
  "target/classes/zero_one/geni/rdd/function/Fn1.class")

(defn- test-namespaces [dirs]
  (sort
   (for [dir  dirs
         file (file-seq (io/file dir))
         :let [path (.getPath ^java.io.File file)]
         :when (string/ends-with? path "_test.clj")]
     (-> path
         (subs (inc (count dir)))
         (string/replace #"\.clj$" "")
         (string/replace "/" ".")
         (string/replace "_" "-")
         symbol))))

(defn- midje-summary
  "The last summary line Midje printed, e.g. `All checks (22) succeeded.`"
  [output]
  (->> (string/split-lines (string/replace output #"\u001B\[[0-9;]*m" ""))
       (filter #(re-find #"^(All checks|FAILURE:|No facts were checked)" %))
       last))

(defn- check-count [summary]
  (let [n #(some-> (re-find % (or summary "")) second parse-long)]
    (or (n #"All checks \((\d+)\)")
        (+ (or (n #"FAILURE: (\d+) checks? failed") 0)
           (or (n #"But (\d+) succeeded") 0)))))

(defn- load-namespace
  "Loads one namespace's facts, keeping Midje's output to itself unless
  something fails. A namespace that fails to load doesn't stop the rest."
  [ns-sym filters]
  (let [out    (java.io.StringWriter.)
        start  (System/nanoTime)
        result (binding [*out*                   out
                         clojure.test/*test-out* out]
                 (try
                   (let [{:keys [failures]} (apply midje.repl/load-facts ns-sym filters)]
                     (cond
                       (nil? (find-ns ns-sym)) {:load-failure? true}
                       (number? failures)      {:failures failures}
                       :else                   {:failures 1}))
                   (catch Throwable e
                     (println (str e))
                     {:load-failure? true})))
        output (str out)
        summary (midje-summary output)]
    (assoc result
           :ns      ns-sym
           :seconds (/ (- (System/nanoTime) start) 1e9)
           :output  output
           :summary summary
           :checks  (check-count summary))))

(defn- passed? [{:keys [load-failure? failures]}]
  (and (not load-failure?) (zero? (or failures 0))))

(defn- report! [{:keys [ns seconds summary load-failure? output] :as result}]
  (let [status (cond load-failure?     "LOAD"
                     (passed? result) "ok"
                     :else            "FAIL")
        detail (cond load-failure?                          "did not load"
                     (string/starts-with? (str summary) "No facts") "no facts selected"
                     :else                                  summary)]
    (when-not (passed? result)
      (print output))
    (println (format "%-4s  %-42s %6.1fs  %s" status ns seconds detail))
    (flush)))

(defn run
  "Runs the tests, and exits with a non-zero status if anything fails.

  Options:
    :dirs     the test directories to scan (default: test)
    :only     the namespaces to load, e.g. [zero-one.geni.dataset-test]
    :include  only run facts with this metadata, e.g. :slow
    :exclude  skip facts with this metadata, e.g. :slow"
  [{:keys [dirs only include exclude] :or {dirs ["test"]}}]
  (when-not (.exists (io/file compiled-java))
    (println "Compiled Java classes not found. Run `clojure -T:build prep` first.")
    (System/exit 1))
  (let [filters (concat (when include [include])
                        (when exclude [(complement exclude)]))
        start   (System/nanoTime)
        results (mapv (fn [ns-sym]
                        (doto (load-namespace ns-sym filters) report!))
                      (or (seq only) (test-namespaces dirs)))
        failed  (remove passed? results)
        seconds (/ (- (System/nanoTime) start) 1e9)
        checks  (reduce + (map :checks results))]
    (println)
    (if (empty? failed)
      (println (format "All %d test namespaces passed: %d checks in %.1fs."
                       (count results) checks seconds))
      (println (format "%d of %d test namespaces failed (%.1fs): %s"
                       (count failed) (count results) seconds
                       (string/join ", " (map :ns failed)))))
    (shutdown-agents)
    (System/exit (if (empty? failed) 0 1))))
