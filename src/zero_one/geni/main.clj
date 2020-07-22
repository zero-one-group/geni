(ns zero-one.geni.main
  (:require
    [clojure.pprint]
    [zero-one.geni.repl :as repl]
    [zero-one.geni.core :as g])
  (:gen-class))

;; TODO: figure out how to get back to 100% test coverage
(defonce spark
  (g/create-spark-session
    {:configs {:spark.testing.memory "3147480000"
               :spark.sql.adaptive.enabled "true"
               :spark.sql.adaptive.coalescePartitions.enabled "true"}
     :checkpoint-dir "resources/checkpoint/"}))

(defn -main [& args]
  (clojure.pprint/pprint (g/spark-conf spark))
  (let [port    (+ 65001 (rand-int 500))
        welcome (repl/spark-welcome-note (.version spark))]
    (println welcome)
    (when (empty? args)
      (repl/launch-repl {:port port :custom-eval '(ns zero-one.geni.main)})
      (System/exit 0))))

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
