(ns zero-one.geni.main
  (:require
    [clojure.pprint]
    [zero-one.geni.core :as g]
    [zero-one.geni.defaults :refer [spark]]
    [zero-one.geni.repl :as repl])
  (:gen-class))

;; Removes the pesky ns warning that takes up the first line of the REPL.
(require '[net.cgrand.parsley.fold])

(defn -main [& _]
  (clojure.pprint/pprint (g/spark-conf @spark))
  (let [port    (+ 65001 (rand-int 500))
        welcome (repl/spark-welcome-note (.version @spark))]
    (println welcome)
    (repl/launch-repl {:port port :custom-eval '(ns zero-one.geni.main)})
    (System/exit 0)))

(comment

  (require '[zero-one.geni.test-resources :refer [melbourne-df]])
  (def dataframe melbourne-df)
  (-> dataframe g/count)
  (-> dataframe g/print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (every-pred (complement :slow) (complement :repl)))

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
