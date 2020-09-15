(ns zero-one.geni.main
  (:require
    [clojure.java.io]
    [clojure.pprint]
    [zero-one.geni.core :as g]
    [zero-one.geni.defaults]
    [zero-one.geni.repl :as repl])
  (:gen-class))

;; Removes the pesky ns warning that takes up the first line of the REPL.
(require '[net.cgrand.parsley.fold])

(def spark
  (future @zero-one.geni.defaults/spark))

(def init-eval
  '(do
     (require 'zero-one.geni.main
              '[zero-one.geni.core :as g]
              '[zero-one.geni.ml :as ml]
              '[zero-one.geni.rdd :as rdd])
     (def spark zero-one.geni.main/spark)))

(defn custom-stream [script-path]
  (try
    (-> (str (slurp script-path) "\nexit\n")
        (.getBytes "UTF-8")
        (java.io.ByteArrayInputStream.))
    (catch Exception _
      (println (str "Cannot find file " script-path "!"))
      (System/exit 1))))

(defn -main [& args]
  (clojure.pprint/pprint (g/spark-conf @spark))
  (println (repl/spark-welcome-note (.version @spark)))
  (let [script-path (if (empty? args) nil (first args))]
    (repl/launch-repl (merge {:port (+ 65001 (rand-int 500))
                              :custom-eval init-eval}
                             (when script-path
                               {:input-stream (custom-stream script-path)}))))
  (System/exit 0))

(comment

  (require '[zero-one.geni.defaults :as defaults])
  (import '(org.apache.spark.streaming Seconds StreamingContext))

  (def streaming-context
    (StreamingContext. (.sparkContext @defaults/spark) (Seconds/apply 1)))

  (def lines
    (.socketTextStream streaming-context "localhost" 9999 g/memory-only))

  (.print lines)
  (.start streaming-context)
  (.awaitTermination streaming-context)
  (.stop streaming-context)


  (require '[zero-one.geni.test-resources :refer [melbourne-df]])
  (def dataframe melbourne-df)
  (-> dataframe g/count)
  (-> dataframe g/print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter :streaming)
  ;(autotest :filter (every-pred (complement :slow) (complement :repl)))

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (import '(org.apache.spark.api.java JavaPairRDD))
  (->> (r/reflect streaming-context)
       :members
       (filter #(= (:name %) 'socketTextStream))
       ;(mapv :name)
       ;sort
       clojure.pprint/pprint)

  0)
