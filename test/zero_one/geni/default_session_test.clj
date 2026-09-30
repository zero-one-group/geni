(ns ^:classic zero-one.geni.default-session-test
  "Geni's default session: requiring Geni starts no Spark, a session that Geni
  didn't create is used as it is, and a session that Geni creates has no
  settings of Geni's own."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
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
          (is (= level (root-level)))))
      (finally
        (System/clearProperty "spark.master")
        (tr/reset-session!)))))

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
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"spark-connect-client-jvm"
                        (g/connect "sc://localhost:15002"))))

;; Requiring Geni can only be checked in a new JVM. It runs without
;; log4j2-test.properties, so that Spark falls back to its own log profile, and
;; its stderr goes to target/fresh-jvm.log.

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

(defn- run-in-fresh-jvm [form]
  (let [java      (str (io/file (System/getProperty "java.home") "bin" "java"))
        jvm-opts  (->> (.getInputArguments (ManagementFactory/getRuntimeMXBean))
                       (remove #(re-find #"^-(javaagent|agentlib|agentpath)" %)))
        classpath (->> (string/split (System/getProperty "java.class.path")
                                     (re-pattern java.io.File/pathSeparator))
                       (remove #(.exists (io/file % "log4j2-test.properties")))
                       (string/join java.io.File/pathSeparator))
        command   (concat [java] jvm-opts ["-cp" classpath "clojure.main" "-e" (pr-str form)])
        process   (-> (ProcessBuilder. ^java.util.List command)
                      (.redirectError (io/file "target/fresh-jvm.log"))
                      .start)
        out       (future (slurp (.getInputStream process)))]
    (when-not (.waitFor process 120 TimeUnit/SECONDS)
      (.destroyForcibly process))
    (some->> (string/split-lines @out)
             (filter #(string/starts-with? % "{"))
             last
             edn/read-string)))

(deftest ^:slow requiring-geni-test
  (let [{:keys [started-on-require? log-level] :as result} (run-in-fresh-jvm fresh-jvm-probe)]
    (is (map? result) "The new JVM failed. See target/fresh-jvm.log.")
    (is (false? started-on-require?))
    (testing "Geni's own session logs at WARN when Spark would log at INFO"
      (is (= "WARN" log-level)))))
