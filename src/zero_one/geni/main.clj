(ns zero-one.geni.main
  (:require
    [clojure.pprint]
    [zero-one.geni.repl]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [spark]])
  (:gen-class))

;; TODO: back to 100% test coverage
(defn -main [& _]
  (clojure.pprint/pprint (g/spark-conf @spark))
  (let [port    (+ 65001 (rand-int 500))
        welcome (zero-one.geni.repl/spark-welcome-note (.version @spark))]
    (println welcome)
    (zero-one.geni.repl/launch-repl port '(ns zero-one.geni.main)))
  (System/exit 0))

(comment

  (require '[zero-one.geni.test-resources :refer [melbourne-df]])
  (def dataframe melbourne-df)
  (-> dataframe g/count)
  (-> dataframe g/print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (complement :slow))

  (require '[clojure.reflect :as r])
  (import '(org.apache.spark.sql Dataset))
  (->> (r/reflect Dataset)
       :members
       (clojure.core/filter #(= (:name %) 'toDF))
       ;(mapv :parameter-types)
       ;(clojure.core/filter #(= (:name %) 'toDF))
       ;clojure.core/sort
       pprint)

  0)
