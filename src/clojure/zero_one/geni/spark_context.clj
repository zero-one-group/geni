(ns zero-one.geni.spark-context
  (:require
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.rdd.unmangle :as unmangle])
  (:import
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark.sql SparkSession)))

(defn java-spark-context [spark]
  (JavaSparkContext/fromSparkContext (.sparkContext spark)))

(defn app-name
  ([] (app-name @defaults/spark))
  ([spark] (-> spark java-spark-context .appName)))

(defmulti binary-files (fn [head & _] (class head)))
(defmethod binary-files :default
  ([path] (binary-files @defaults/spark path))
  ([path num-partitions] (binary-files @defaults/spark path num-partitions)))
(defmethod binary-files SparkSession
  ([spark path] (.binaryFiles (java-spark-context spark) path))
  ([spark path num-partitions]
   (.binaryFiles (java-spark-context spark) path num-partitions)))

(defn broadcast
  ([value] (broadcast @defaults/spark value))
  ([spark value] (-> spark java-spark-context (.broadcast value))))

(defn checkpoint-dir
  ([] (checkpoint-dir @defaults/spark))
  ([spark]
   (-> spark java-spark-context .getCheckpointDir interop/optional->nillable)))

(defn conf
  ([] (conf @defaults/spark))
  ([spark] (-> spark java-spark-context .getConf interop/spark-conf->map)))

(defn default-min-partitions
  ([] (default-min-partitions @defaults/spark))
  ([spark] (-> spark java-spark-context .defaultMinPartitions)))

(defn default-parallelism
  ([] (default-parallelism @defaults/spark))
  ([spark] (-> spark java-spark-context .defaultParallelism)))

(defn empty-rdd
  ([] (empty-rdd @defaults/spark))
  ([spark] (-> spark java-spark-context .emptyRDD)))

(defn jars
  ([] (jars @defaults/spark))
  ([spark] (->> spark java-spark-context .jars (into []))))

(defn local?
  ([] (local? @defaults/spark))
  ([spark] (-> spark java-spark-context .isLocal)))
(def is-local local?)

(defn local-property
  ([k] (local-property @defaults/spark k))
  ([spark k] (-> spark java-spark-context (.getLocalProperty k))))

(defn master
  ([] (master @defaults/spark))
  ([spark] (-> spark java-spark-context .master)))

;; TODO: support min-partitions arg
(defn parallelise
  ([data] (parallelise @defaults/spark data))
  ([spark data] (-> spark
                    java-spark-context
                    (.parallelize data)
                    unmangle/unmangle-name)))
(def parallelize parallelise)

(defn parallelise-doubles
  ([data] (parallelise-doubles @defaults/spark data))
  ([spark data]
   (-> spark
       java-spark-context
       (.parallelizeDoubles (clojure.core/map double data))
       unmangle/unmangle-name)))
(def parallelize-doubles parallelise-doubles)

(defn parallelise-pairs
  ([data] (parallelise-pairs @defaults/spark data))
  ([spark data]
   (-> spark
       java-spark-context
       (.parallelizePairs (clojure.core/map interop/->scala-tuple2 data))
       unmangle/unmangle-name)))
(def parallelize-pairs parallelise-pairs)

(defn persistent-rdds
  ([] (persistent-rdds @defaults/spark))
  ([spark] (->> spark java-spark-context .getPersistentRDDs (into {}))))

(defn resources
  ([] (resources @defaults/spark))
  ([spark] (->> spark java-spark-context .resources (into {}))))

(defn spark-context
  ([] (spark-context @defaults/spark))
  ([spark] (-> spark java-spark-context .sc)))
(def sc spark-context)

(defn spark-home
  ([] (spark-home @defaults/spark))
  ([spark] (-> spark java-spark-context .getSparkHome interop/optional->nillable)))

(defmulti text-file (fn [head & _] (class head)))
(defmethod text-file :default
  ([path] (text-file @defaults/spark path))
  ([path min-partitions] (text-file @defaults/spark path min-partitions)))
(defmethod text-file SparkSession
  ([spark path] (-> spark java-spark-context (.textFile path)))
  ([spark path min-partitions] (-> spark java-spark-context (.textFile path min-partitions))))

(defn version
  ([] (version @defaults/spark))
  ([spark] (-> spark java-spark-context .version)))

(defmulti whole-text-files (fn [head & _] (class head)))
(defmethod whole-text-files :default
  ([path] (whole-text-files @defaults/spark path))
  ([path min-partitions] (whole-text-files @defaults/spark path min-partitions)))
(defmethod whole-text-files SparkSession
  ([spark path]
   (.wholeTextFiles (java-spark-context spark) path))
  ([spark path min-partitions]
   (.wholeTextFiles (java-spark-context spark) path min-partitions)))

;; Broadcast
(def value (memfn value))
