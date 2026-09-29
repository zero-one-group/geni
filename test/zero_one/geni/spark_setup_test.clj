(ns zero-one.geni.spark-setup-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
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
