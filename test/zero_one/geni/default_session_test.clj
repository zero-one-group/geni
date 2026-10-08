(ns ^:classic zero-one.geni.default-session-test
  "Geni's default session: requiring Geni starts no Spark, a session that Geni
  didn't create is used as it is, and a session that Geni creates has no
  settings of Geni's own, but for the serializer of a local one."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.spark]
   [zero-one.geni.test-resources :as tr :refer [spark]])
  (:import
   (java.lang.management ManagementFactory)
   (java.util.concurrent TimeUnit)
   (org.apache.logging.log4j LogManager)
   (org.apache.spark.sql SparkSession)))

(defn- root-level []
  (str (.getLevel (LogManager/getRootLogger))))

(defn- geni-configs [session]
  (select-keys (g/spark-conf session) [:spark.sql.adaptive.enabled
                                       :spark.sql.adaptive.coalescePartitions.enabled]))

(deftest existing-session-test
  (let [level (root-level)]
    (try
      (tr/stop-session!)
      (let [own (.. (SparkSession/builder)
                    (appName "Someone Else's App")
                    (master "local[1]")
                    (config "spark.sql.shuffle.partitions" "3")
                    getOrCreate)]
        (testing "Geni uses a session it didn't create"
          (is (identical? own @spark))
          (is (identical? own (.sparkSession (g/range 3)))))
        (testing "and leaves it as it is"
          (is (= "Someone Else's App" (g/app-name)))
          (is (= "3" (-> own .conf (.get "spark.sql.shuffle.partitions"))))
          (is (nil? (g/checkpoint-dir)))
          (is (empty? (geni-configs own)))
          (is (nil? (:spark.serializer (g/spark-conf own))))
          (is (= level (root-level)))))
      (finally
        (tr/reset-session!)))))

(deftest created-session-test
  (let [level (root-level)]
    (try
      (tr/stop-session!)
      ;; As spark-submit would set it.
      (System/setProperty "spark.master" "local[2]")
      (let [session @spark]
        (testing "Geni creates a session when there's none running"
          (is (identical? session @spark))
          (is (= "Geni App" (g/app-name)))
          (is (= "local[2]" (g/master))))
        (testing "with no settings of its own"
          (is (nil? (g/checkpoint-dir)))
          (is (empty? (geni-configs session)))
          ;; log4j2-test.properties is on the classpath.
          (is (= level (root-level))))
        (testing "but for the serializer of a local session"
          (is (= "zero_one.geni.rdd.ClojureSerializer"
                 (:spark.serializer (g/spark-conf session))))))
      (finally
        (System/clearProperty "spark.master")
        (tr/reset-session!)))))

(deftest own-serializer-test
  (try
    (tr/stop-session!)
    (System/setProperty "spark.serializer" "org.apache.spark.serializer.JavaSerializer")
    (testing "Geni keeps a spark.serializer that's set already"
      (is (= "org.apache.spark.serializer.JavaSerializer"
             (:spark.serializer (g/spark-conf @spark)))))
    (finally
      (System/clearProperty "spark.serializer")
      (tr/reset-session!))))

(deftest local-master-test
  (let [local-master? #'zero-one.geni.spark/local-master?]
    (is (every? local-master? ["local" "local[2]" "local[*]" "local[4,2]"]))
    (is (not-any? local-master? [nil "local-cluster[2,1,1024]" "spark://host:7077" "yarn"]))))

(deftest set-default-session-test
  (let [other (.newSession ^SparkSession @spark)]
    (try
      (is (identical? other (g/set-default-session! other)))
      (is (identical? other @spark))
      (is (identical? other (.sparkSession (g/range 1))))
      (finally
        (g/set-default-session! nil)))
    (is (not (identical? other @spark)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Expected a SparkSession"
                          (g/set-default-session! "local[*]")))))

(deftest connect-needs-the-client-test
  (let [e (try (g/connect "sc://localhost:15002/;token=secret")
               (catch clojure.lang.ExceptionInfo e e))]
    (is (re-find #"spark-connect-client-jvm" (str (ex-message e))))
    (testing "and leaves out the URL, which can hold a token"
      (is (not (string/includes? (pr-str (ex-data e)) "secret"))))))

;; Requiring Geni and the missing JVM flags can only be checked in new JVMs.
;; They run without log4j2-test.properties, so that Spark falls back to its own
;; log profile, and their stderr goes to target/fresh-jvm-*.log.

(def ^:private fresh-jvm-probe
  '(do
     (require 'zero-one.geni.core)
     (let [started? (or (.isDefined (org.apache.spark.sql.SparkSession/getActiveSession))
                        (.isDefined (org.apache.spark.sql.SparkSession/getDefaultSession)))]
       @zero-one.geni.defaults/spark
       (prn {:started-on-require? started?
             :log-level           (str (.getLevel (org.apache.logging.log4j.LogManager/getRootLogger)))})
       (shutdown-agents)
       (System/exit 0))))

(defn- run-in-fresh-jvm [form log & {:keys [drop-flag?] :or {drop-flag? (constantly false)}}]
  (let [java      (str (io/file (System/getProperty "java.home") "bin" "java"))
        jvm-opts  (->> (.getInputArguments (ManagementFactory/getRuntimeMXBean))
                       (remove #(re-find #"^-(javaagent|agentlib|agentpath)" %))
                       (remove drop-flag?))
        classpath (->> (string/split (System/getProperty "java.class.path")
                                     (re-pattern java.io.File/pathSeparator))
                       (remove #(.exists (io/file % "log4j2-test.properties")))
                       (string/join java.io.File/pathSeparator))
        command   (concat [java] jvm-opts ["-cp" classpath "clojure.main" "-e" (pr-str form)])
        process   (-> (ProcessBuilder. ^java.util.List command)
                      (.redirectError (io/file log))
                      .start)
        out       (future (slurp (.getInputStream process)))]
    (when-not (.waitFor process 120 TimeUnit/SECONDS)
      (.destroyForcibly process))
    (some->> (string/split-lines @out)
             (filter #(string/starts-with? % "{"))
             last
             edn/read-string)))

(def ^:private missing-flags-probe
  '(do
     (require 'zero-one.geni.spark)
     (try
       (zero-one.geni.spark/create-spark-session {})
       (prn {:started? true})
       (catch clojure.lang.ExceptionInfo e
         (prn {:message (ex-message e) :missing (:missing-jvm-flags (ex-data e))}))
       (finally
         (shutdown-agents)
         (System/exit 0)))))

(def ^:private fresh-jvms
  "Both new JVMs, started together, since each takes seconds to start Spark."
  (delay
    {:require (future (run-in-fresh-jvm fresh-jvm-probe "target/fresh-jvm-require.log"))
     :flags   (future (run-in-fresh-jvm missing-flags-probe "target/fresh-jvm-flags.log"
                                        :drop-flag? #(string/starts-with? % "--add-opens=")))}))

(deftest requiring-geni-test
  (let [{:keys [started-on-require? log-level] :as result} @(:require @fresh-jvms)]
    (is (map? result) "The new JVM failed. See target/fresh-jvm-require.log.")
    (is (false? started-on-require?))
    (testing "Geni's own session logs at WARN when Spark would log at INFO"
      (is (= "WARN" log-level)))
    (testing "from the start, so that Spark's INFO lines as it starts don't show"
      (is (not (re-find #" INFO " (slurp "target/fresh-jvm-require.log")))))))

(deftest missing-jvm-flags-test
  ;; Without the --add-opens flags, Spark 3.5 doesn't start, and Spark 4.2
  ;; starts but can fail later.
  (let [{:keys [started? message missing] :as result} @(:flags @fresh-jvms)
        flag "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
    (is (map? result) "The new JVM failed. See target/fresh-jvm-flags.log.")
    (if started?
      (testing "Geni warns about the flags that Spark's launcher sets and the JVM lacks"
        (let [log (slurp "target/fresh-jvm-flags.log")]
          (is (re-find #"WARN .*The JVM lacks flags" log))
          (is (string/includes? log flag))))
      (testing "Spark's error names the flags that its launcher sets and the JVM lacks"
        (is (some #{flag} missing))
        (is (string/includes? (str message) flag))
        (is (string/includes? (str message) "README"))))))
