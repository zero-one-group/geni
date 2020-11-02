(ns user
  (:require [zero-one.geni.core :as g]
            [zero-one.geni.defaults :as defaults])
  (:gen-class))

(defn -main
  []
  (println (g/spark-conf @defaults/spark)))
