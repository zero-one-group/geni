(ns zero-one.geni.streaming-test
  (:require
    [clojure.edn :as edn]
    [clojure.string :as string]
    [clojure.java.io :as io]
    [midje.sweet :refer [facts fact throws =>]]
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.streaming :as streaming])
  (:import
    (org.apache.spark.streaming Duration StreamingContext)))

(defn create-random-temp-file! [file-name]
  (let [temp-dir (System/getProperty "java.io.tmpdir")
        file     (->> [temp-dir (java.util.UUID/randomUUID) file-name]
                      (string/join "/")
                      io/file)]
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
                          @defaults/spark
                          (streaming/milliseconds (:duration-ms opts 100)))
        read-file      (create-random-temp-file! "read.txt")
        write-file     (create-random-temp-file! "write")
        input-stream   (streaming/text-file-stream
                         context
                         (-> read-file .getParent .toString))
        d-stream        ((:fn opts identity) input-stream)]
    (spit read-file "")
    (streaming/save-as-text-files! d-stream (.toString write-file))
    @(streaming/start! context)
    (Thread/sleep (* 2 (:duration-ms opts 100)))
    (spit read-file (str (:content opts "Hello World!")))
    (streaming/await-termination! context)
    @(streaming/stop! context)
    (written-content write-file)))

(facts "On DStream testing" :streaming
  (stream-results {:content (range 10) :fn streaming/cache})
  => (str (range 10) "\n")
  (stream-results {:content (range 10)
                   :fn #(streaming/checkpoint % (streaming/milliseconds 200))})
  => (str (range 10) "\n")
  (stream-results {:content "abc\ndef" :fn streaming/count})
  => #(->> (string/split % #"\n") (map edn/read-string) (every? int?))
  (stream-results {:content "abc\ndef" :fn streaming/glom})
  ;; TODO: chain with other methods
  => #(string/includes? % "Ljava.lang.String")
  (stream-results {:content "abc\ndef" :fn streaming/persist})
  => "abc\ndef\n"
  (stream-results {:content "abc\ndef"
                   :fn #(streaming/persist % streaming/memory-only)})
  => "abc\ndef\n"
  (stream-results {:content (range 10) :fn streaming/print})
  => (throws java.lang.NullPointerException)
  (stream-results {:content (range 10) :fn #(streaming/print % 2)})
  => (throws java.lang.NullPointerException)
  (stream-results {:content (range 10) :fn #(streaming/union % %)})
  => "(0 1 2 3 4 5 6 7 8 9)\n(0 1 2 3 4 5 6 7 8 9)\n"
  (stream-results {:content (range 10) :fn streaming/slide-duration})
  ;;; TODO: chain with other method
  => (throws java.lang.IllegalArgumentException))

(facts "On durations" :streaming
  (fact "durations instantiatable"
    (mapv
      (fn [f] (f 1) => (partial instance? Duration))
      [streaming/milliseconds streaming/seconds streaming/minutes])))

(facts "On StreamingContext" :streaming
  (fact "streaming context instantiatable"
    (streaming/streaming-context @defaults/spark (streaming/seconds 1))
    => (partial instance? StreamingContext)))
