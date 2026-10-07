(ns ^:classic zero-one.geni.spark-setup-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark]
   [zero-one.geni.test-resources :refer [spark melbourne-df]])
  (:import
   (java.lang.management ManagementFactory)
   (org.apache.spark.launcher JavaModuleOptions)
   (org.apache.spark.sql Dataset SparkSession)))

(deftest test-spark-session-and-dataframe-test
  (is (instance? SparkSession @spark))
  (is (instance? Dataset (melbourne-df)))
  (is (= ((-> @spark .conf .getAll interop/scala-map->map) "spark.master") "local[*]"))
  (is (clojure.string/includes? (-> @spark .sparkContext .getConf g/to-debug-string) "spark.app.id"))
  (is (= {:spark.master   "local[*]",
          :spark.app.name "Geni App"}
         (select-keys (g/spark-conf @spark) [:spark.master
                                             :spark.app.name
                                             :spark.sql.adaptive.enabled
                                             :spark.sql.adaptive.coalescePartitions.enabled]))))

(deftest test-primary-key-is-the-product-test
  (is (= 13580
         (-> (melbourne-df)
             (g/with-column
               "entry_id"
               (g/concat "Address" (g/lit "::") "Date" (g/lit "::") "SellerG"))
             (g/select "entry_id")
             g/distinct
             g/count))))

(deftest jvm-flags-test
  ;; The Spark aliases in deps.edn copy the JDK flags that Spark's launcher
  ;; uses, and a new Spark release can add more.
  (let [jvm-args (set (.getInputArguments (ManagementFactory/getRuntimeMXBean)))]
    (is (= [] (remove jvm-args (clojure.string/split (JavaModuleOptions/defaultModuleOptions) #" "))))))

(deftest datasketches-jdk-check-test
  ;; Spark 4.2's spark-catalyst has its own copy of a datasketches-memory
  ;; class, whose JDK check takes JDK 25. It only works ahead of
  ;; datasketches-memory on the classpath, which the Spark 4 setups make sure
  ;; of by listing spark-catalyst. Without it, Spark's sketch functions fail on
  ;; JDK 25. Spark 3.5 has no copy.
  (let [copies (->> "org/apache/datasketches/memory/internal/ResourceImpl.class"
                    (.getResources (ClassLoader/getSystemClassLoader))
                    enumeration-seq
                    (mapv str))]
    (is (or (< (count copies) 2) (clojure.string/includes? (first copies) "spark-catalyst"))
        (str "datasketches' own JDK check comes first: " copies))))

(deftest launcher-opens-test
  (testing "Geni's copy of the launcher's --add-opens, for the Spark Connect client, is Spark's"
    (is (= (sort (re-seq #"--add-opens=\S+" (JavaModuleOptions/defaultModuleOptions)))
           (sort @#'zero-one.geni.spark/launcher-opens))))
  (testing "this JVM has them all"
    (is (empty? (#'zero-one.geni.spark/missing-opens @#'zero-one.geni.spark/launcher-opens))))
  (testing "a flag it lacks is found, and a module or package it doesn't have is skipped"
    ;; JDK 24 and later have no sun.security.action, which Spark still opens.
    (is (= ["--add-opens=java.base/java.lang.ref=ALL-UNNAMED"]
           (#'zero-one.geni.spark/missing-opens
            ["--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.lang.ref=ALL-UNNAMED"
             "--add-opens=java.base/no.such.package=ALL-UNNAMED"
             "--add-opens=no.such.module/no.such.package=ALL-UNNAMED"])))))
