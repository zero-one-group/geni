(ns zero-one.geni.spark-context
  (:require
   [potemkin :refer [import-fn]]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.rdd.unmangle :as unmangle])
  (:import
   (org.apache.spark.api.java JavaSparkContext)
   (org.apache.spark.sql SparkSession)))

(defn java-spark-context
  "Converts a SparkSession to a JavaSparkContext."
  [spark]
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

(defn get-checkpoint-dir
  ([] (get-checkpoint-dir @defaults/spark))
  ([spark]
   (-> spark java-spark-context .getCheckpointDir interop/optional->nillable)))

(defn get-conf
  ([] (get-conf @defaults/spark))
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

(defn is-local
  ([] (is-local @defaults/spark))
  ([spark] (-> spark java-spark-context .isLocal)))

(defn get-local-property
  ([k] (get-local-property @defaults/spark k))
  ([spark k] (-> spark java-spark-context (.getLocalProperty k))))

(defn master
  ([] (master @defaults/spark))
  ([spark] (-> spark java-spark-context .master)))

;; TODO: support min-partitions arg
(defn parallelize
  ([data] (parallelize @defaults/spark data))
  ([spark data] (-> spark
                    java-spark-context
                    (.parallelize data)
                    unmangle/unmangle-name)))

(defn parallelize-doubles
  ([data] (parallelize-doubles @defaults/spark data))
  ([spark data]
   (-> spark
       java-spark-context
       (.parallelizeDoubles (map double data))
       unmangle/unmangle-name)))

(defn parallelize-pairs
  ([data] (parallelize-pairs @defaults/spark data))
  ([spark data]
   (-> spark
       java-spark-context
       (.parallelizePairs (map interop/->scala-tuple2 data))
       unmangle/unmangle-name)))

(defn get-persistent-rd-ds
  ([] (get-persistent-rd-ds @defaults/spark))
  ([spark] (->> spark java-spark-context .getPersistentRDDs (into {}))))

(defn resources
  ([] (resources @defaults/spark))
  ([spark] (->> spark java-spark-context .resources (into {}))))

(defn sc
  ([] (sc @defaults/spark))
  ([spark] (-> spark java-spark-context .sc)))

(defn get-spark-home
  ([] (get-spark-home @defaults/spark))
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
(def value
  "memfn of value"
  (memfn value))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.spark-context
 [(-> docs/spark-docs :methods :spark :context)])

;; Aliases
(import-fn get-checkpoint-dir checkpoint-dir)
(import-fn get-conf conf)
(import-fn get-local-property local-property)
(import-fn get-persistent-rd-ds get-persistent-rdds)
(import-fn get-persistent-rd-ds persistent-rdds)
(import-fn get-spark-home spark-home)
(import-fn is-local local?)
(import-fn parallelize parallelise)
(import-fn parallelize-doubles parallelise-doubles)
(import-fn parallelize-pairs parallelise-pairs)
(import-fn sc spark-context)
