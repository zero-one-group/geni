(ns zero-one.geni.spark-context
  (:require
   [zero-one.geni.utils :refer [import-fn]]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.rdd.unmangle :as unmangle]
   [zero-one.geni.spark :as spark])
  (:import
   (clojure.lang Reflector)
   (org.apache.spark.sql SparkSession)))

(defn java-spark-context
  "Converts a SparkSession to a JavaSparkContext. Only classic sessions have
  one, and a Spark Connect session throws an error that says so."
  [spark]
  ;; Looked up when it's called, since a Spark Connect client doesn't have it.
  (Reflector/invokeStaticMethod "org.apache.spark.api.java.JavaSparkContext"
                                "fromSparkContext"
                                (object-array [(spark/spark-context spark)])))

(defn app-name
  ([] (app-name @defaults/spark))
  ([spark] (-> spark java-spark-context .appName)))

(defn binary-files
  {:arglists '([path] [path num-partitions] [spark path] [spark path num-partitions])}
  [& args]
  (let [[spark [path num-partitions]] (defaults/session-and-args args)]
    (if num-partitions
      (.binaryFiles (java-spark-context spark) path num-partitions)
      (.binaryFiles (java-spark-context spark) path))))

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

(defn text-file
  {:arglists '([path] [path min-partitions] [spark path] [spark path min-partitions])}
  [& args]
  (let [[spark [path min-partitions]] (defaults/session-and-args args)]
    (if min-partitions
      (.textFile (java-spark-context spark) path min-partitions)
      (.textFile (java-spark-context spark) path))))

(defn version
  ([] (version @defaults/spark))
  ([spark] (.version ^SparkSession spark)))

(defn whole-text-files
  {:arglists '([path] [path min-partitions] [spark path] [spark path min-partitions])}
  [& args]
  (let [[spark [path min-partitions]] (defaults/session-and-args args)]
    (if min-partitions
      (.wholeTextFiles (java-spark-context spark) path min-partitions)
      (.wholeTextFiles (java-spark-context spark) path))))

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
