(ns zero-one.geni.streaming-test
  (:require
    [clojure.string :as string]
    [clojure.java.io :as io]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.spark :as spark]
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.streaming :as streaming])
  (:import
    (org.apache.spark.streaming Duration StreamingContext)))

(defn create-random-file! [file-name]
  (let [file (io/file (str "target/" (java.util.UUID/randomUUID) "/" file-name))]
    (io/make-parents file)
    file))

(defn written-content [write-file]
  (let [files         (-> write-file .getParent io/file file-seq)
        slurped-texts (->> files
                           (filter #(.isFile %))
                           (map #(.toString %))
                           (filter #(string/includes? % "/part"))
                           (map slurp))]
    (string/join slurped-texts)))

(defn stream-results [opts]
  (let [context         (streaming/streaming-context
                          (spark/create-spark-session {})
                          (:duration opts (streaming/milliseconds 100)))
        read-file      (create-random-file! "read.txt")
        write-file     (create-random-file! "write")
        input-stream   (streaming/text-file-stream
                         context
                         (-> read-file .getParent .toString))
        d-stream        ((:d-stream-fn opts identity) input-stream)]
    (spit read-file "")
    (streaming/save-as-text-files! d-stream (.toString write-file))
    @(streaming/start! context)
    (spit read-file (str (:content opts "Hello World!")))
    (streaming/await-termination! context)
    @(streaming/stop! context)
    (written-content write-file)))

(facts "On DStream testing" :streaming
  (stream-results {:content (range 10)}) => (str (range 10) "\n"))

(facts "On durations" :streaming
  (fact "durations instantiatable"
    (mapv
      (fn [f] (f 1) => (partial instance? Duration))
      [streaming/milliseconds streaming/seconds streaming/minutes])))

(facts "On StreamingContext" :streaming
  (fact "streaming context instantiatable"
    (streaming/streaming-context @defaults/spark (streaming/seconds 1))
    => (partial instance? StreamingContext)))
