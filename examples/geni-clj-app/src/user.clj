(ns user
  (:require [zero-one.geni.core :as g])
  (:gen-class))

(defn -main
  "Convert csv file to parquet"
  [& args]
  (if (not= 1 (count args))
    (do (println (str "Provide a single filename: " (count args)))
        (System/exit 1))
    (let [arg (first args)]
      (-> (g/read-csv! (str arg ".csv"))
          (g/write-parquet! (str arg ".parquet"))))))
