(ns zero-one.geni.main
  (:require
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
              '[zero-one.geni.ml :as ml])
     (def spark zero-one.geni.main/spark)))

(defn -main [& _]
  (clojure.pprint/pprint (g/spark-conf @spark))
  (let [port    (+ 65001 (rand-int 500))
        welcome (repl/spark-welcome-note (.version @spark))]
    (println welcome)
    (repl/launch-repl {:port port :custom-eval init-eval})
    (System/exit 0)))

(comment

  (require '[zero-one.geni.test-resources :refer [melbourne-df]])
  (def dataframe melbourne-df)
  (-> dataframe g/count)
  (-> dataframe g/print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (every-pred (complement :slow) (complement :repl)))

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (import '(org.apache.spark.sql Dataset))
  (->> (r/reflect (.write dataframe))
       :members
       (clojure.core/filter #(= (:name %) 'partitionBy))
       ;(mapv :parameter-types)
       ;(clojure.core/filter #(= (:name %) 'toDF))
       ;clojure.core/sort
       clojure.pprint/pprint)

  0)
