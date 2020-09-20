(ns zero-one.geni.streaming-test
  (:require
    [clojure.java.io :as io]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.spark :as spark]
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.streaming :as streaming]
    [zero-one.geni.test-resources :refer [create-temp-file!]])
  (:import
    (org.apache.spark.streaming Duration StreamingContext)))


(defn create-random-file! [file-name]
  (let [file (io/file (str "target/" (java.util.UUID/randomUUID) "/" file-name))]
    (io/make-parents file)
    file))

(defn stream-results [opts]
  (let [new-context (streaming/streaming-context
                      (spark/create-spark-session {})
                      (:duration opts (streaming/milliseconds 100)))
        read-file  (create-random-file! "/read.txt")
        write-file (create-random-file! "/write")
        d-stream   (streaming/text-file-stream
                     new-context
                     (-> read-file .getParent .toString))]
    (spit read-file "=== START ===\n")
    (streaming/save-as-text-files! d-stream (-> write-file .toString))
    (streaming/start! new-context)
    ;; TODO: must wait until tihs starts
    (doall
      (for [line (:lines opts)]
        (do
          (Thread/sleep 200)
          (spit read-file (str line "\n") :append true))))
    (spit read-file "=== STOP ===\n" :append true)
    (streaming/await-termination! new-context)
    @(streaming/stop! new-context)
    (print write-file)
    ;; TODO: grab all filse that has part in it
    (-> write-file .getParent io/file file-seq)))

(facts "On DStream testing" :streaming
  (stream-results {:lines (range 10)}) => 1)

(facts "On durations" :streaming
  (fact "durations instantiatable"
    (mapv
      (fn [f] (f 1) => (partial instance? Duration))
      [streaming/milliseconds streaming/seconds streaming/minutes])))

(facts "On StreamingContext" :streaming
  (fact "streaming context instantiatable"
    (streaming/streaming-context @defaults/spark (streaming/seconds 1))
    => (partial instance? StreamingContext)))
