(ns zero-one.geni.streaming-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.streaming :as stream])
  (:import
    (org.apache.spark.streaming Duration StreamingContext)))

(facts "On durations" :streaming
  (fact "durations instantiatable"
    (mapv
      (fn [f] (f 1) => (partial instance? Duration))
      [stream/milliseconds stream/seconds stream/minutes])))

(facts "On StreamingContext" :streaming
  (fact "streaming context instantiatable"
    (stream/streaming-context @defaults/spark (stream/seconds 1))
    => (partial instance? StreamingContext)))
