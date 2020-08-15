(ns zero-one.geni.spark
  (:require
    [clojure.walk]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql SparkSession)
    (org.apache.log4j Logger Level)))

(defn- non-verbose-get-or-create-session [builder log-level]
  (let [logger         (Logger/getLogger "org")
        original-level (.getLevel logger)
        session        (do
                         (.setLevel logger (Level/toLevel log-level))
                         (.getOrCreate builder))]
    (.setLevel logger original-level)
    session))

(defn create-spark-session [{:keys [app-name master configs log-level checkpoint-dir]
                             :or   {app-name  "Geni App"
                                    master    "local[*]"
                                    configs   {}
                                    log-level "WARN"}}]
  (let [unconfigured (.. (SparkSession/builder)
                         (appName app-name)
                         (master master))
        configured   (reduce
                       (fn [s [k v]] (.config s (name k) v))
                       unconfigured
                       configs)
        session      (non-verbose-get-or-create-session configured log-level)
        context      (.sparkContext session)]
    (.setLogLevel context log-level)
    (clojure.core/when checkpoint-dir
      (.setCheckpointDir context checkpoint-dir))
    session))

(defn spark-conf [spark-session]
  (->> spark-session
       .sparkContext
       .getConf
       .getAll
       (map interop/scala-tuple->vec)
       (into {})
       clojure.walk/keywordize-keys))
