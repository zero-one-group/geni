(ns zero-one.geni.streaming-test
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [midje.sweet :refer [facts fact throws =>]]
    [zero-one.geni.aot-functions :as aot]
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.streaming :as streaming])
  (:import
    (org.apache.spark.streaming Duration StreamingContext)
    (org.apache.spark.streaming.api.java JavaDStream JavaStreamingContext)
    (org.apache.spark.streaming.dstream DStream)))

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
        dstream        ((:fn opts identity) input-stream)]
    (spit read-file "")
    (streaming/save-as-text-files! dstream (.toString write-file))
    @(streaming/start! context)
    (Thread/sleep (:sleep-ms opts 50))
    (spit read-file (str (:content opts "Hello World!")))
    ((:action-fn opts identity) dstream)
    (Thread/sleep (:sleep-ms opts 50))
    (streaming/await-termination! context)
    @(streaming/stop! context)
    (let [result      (written-content write-file)
          n-retries   (:n-retries opts 0)
          max-retries (:max-tries opts 5)
          expected    (:expected opts result)
          finish-fn   (:finish-fn opts identity)]
      (if (and (or (= result "")
                   (not= (finish-fn result) expected))
               (< n-retries max-retries))
        (do
          (println "Retrying stream-results ...")
          (stream-results (assoc opts :n-retries (inc n-retries))))
        (finish-fn result)))))

(def dummy-text
  (slurp "test/resources/rdd.txt"))

(facts "On DStream methods" :streaming
  (stream-results
    {:content dummy-text
     :fn #(-> %
              (streaming/map-to-pair aot/to-pair)
              (streaming/flat-map-values aot/to-pair)
              streaming/->java-dstream)
     :finish-fn #(-> % (string/split #"\n") distinct count)
     :expected 6})
  => 6
  (stream-results
    {:content dummy-text
     :fn #(-> %
              (streaming/flat-map-to-pair aot/split-spaces-and-pair)
              (streaming/reduce-by-key +)
              streaming/->java-dstream)
     :finish-fn #(-> % (string/split #"\n") distinct count)
     :expected 23})
  => 23
  (stream-results
    {:content dummy-text
     :fn #(-> %
              (streaming/flat-map aot/split-spaces)
              (streaming/filter aot/equals-lewis))
     :finish-fn #(set (string/split % #"\n"))
     :expected #{"Lewis"}})
  => #{"Lewis"}
  (stream-results
    {:content dummy-text
     :fn (comp streaming/count streaming/count-by-value)})
  => #(string/includes? % "0\n")
  (stream-results
    {:content dummy-text
     :fn (comp streaming/count #(streaming/count-by-value % 1))})
  => #(string/includes? % "0\n")
  (stream-results
    {:content dummy-text
     :action-fn #(let [now (System/currentTimeMillis)]
                   (assert (nil? (streaming/compute % (+ 100 now)))))})
  => string?
  (stream-results
    {:content dummy-text
     :fn #(streaming/flat-map % aot/split-spaces)
     :finish-fn #(count (string/split % #"\n"))
     :expected 522})
  => 522)

(facts "On DStream testing" :streaming
  (stream-results
    {:content (range 10)
     :fn streaming/cache})
  => (str (range 10) "\n")
  (stream-results
    {:content (range 10)
     :fn #(streaming/checkpoint % (streaming/milliseconds 200))})
  => (str (range 10) "\n")
  (stream-results
    {:content "abc\ndef"
     :fn streaming/count})
  => #(->> (string/split % #"\n") (map edn/read-string) (every? int?))
  (stream-results
    {:content "abc\ndef"
     :fn streaming/glom})
  => #(string/includes? % "[abc")
  (stream-results
    {:content "abc\ndef"
     :fn streaming/persist
     :expected "abc\ndef\n"})
  => "abc\ndef\n"
  (stream-results
    {:content "abc\ndef"
     :fn #(streaming/persist % streaming/memory-only)})
  => "abc\ndef\n"
  (stream-results
    {:content (range 10)
     :fn streaming/print})
  => (throws java.lang.NullPointerException)
  (stream-results
    {:content (range 10)
     :fn #(streaming/print % 2)})
  => (throws java.lang.NullPointerException)
  (stream-results
    {:content (range 10)
     :fn #(streaming/union % %)})
  => "(0 1 2 3 4 5 6 7 8 9)\n(0 1 2 3 4 5 6 7 8 9)\n"
  (stream-results
    {:content (range 10)
     :fn streaming/slide-duration})
  ;;; TODO: chain with other method
  => (throws java.lang.IllegalArgumentException))

(facts "On durations" :streaming
  (fact "durations instantiatable"
    (mapv
      (fn [f] (f 1) => (partial instance? Duration))
      [streaming/milliseconds streaming/seconds streaming/minutes])))

(facts "On StreamingContext" :streaming
  (let [context (streaming/streaming-context @defaults/spark (streaming/seconds 1))]
    (fact "streaming context instantiatable"
      context => (partial instance? JavaStreamingContext))
    (fact "retrieving context from a dstream"
      (let [dstream (streaming/socket-text-stream context
                                            "localhost"
                                            9999
                                            streaming/memory-only)]
        dstream => (partial instance? JavaDStream)
        (streaming/dstream dstream) => (partial instance? DStream)
        (streaming/context dstream) => (partial instance? StreamingContext)))))
