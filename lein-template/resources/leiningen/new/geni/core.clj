(ns {{namespace}}.core
  (:require
    [clojure.pprint]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.repl :as repl])
  (:gen-class))

;; Removes the pesky ns warning that takes up the first line of the REPL.
(require '[net.cgrand.parsley.fold])

{{! Change mustache delimiter to <% and %>}}
{{=<% %>=}}
(defonce spark (future (g/create-spark-session {<%#dataproc?%>:master "yarn"<%/dataproc?%>})))
<%! Reset mustache delimiter %>
<%={{ }}=%>

(def training-set
  (future
    (g/table->dataset
      @spark
      [[0 "a b c d e spark"  1.0]
       [1 "b d"              0.0]
       [2 "spark f g h"      1.0]
       [3 "hadoop mapreduce" 0.0]]
      [:id :text :label])))

(def pipeline
  (ml/pipeline
    (ml/tokenizer {:input-col :text
                   :output-col :words})
    (ml/hashing-tf {:num-features 1000
                    :input-col :words
                    :output-col :features})
    (ml/logistic-regression {:max-iter 10
                             :reg-param 0.001})))

(def test-set
  (future
    (g/table->dataset
      @spark
      [[4 "spark i j k"]
       [5 "l m n"]
       [6 "spark hadoop spark"]
       [7 "apache hadoop"]]
      [:id :text])))

(defn -main [& _]
  (clojure.pprint/pprint (g/spark-conf @spark))
  (let [model (ml/fit @training-set pipeline)]
    (-> @test-set
        (ml/transform model)
        (g/select :id :text :probability :prediction)
        g/show))
  (let [port    (+ 65001 (rand-int 500))
        welcome (repl/spark-welcome-note (.version @spark))]
    (println welcome)
    (repl/launch-repl {:port port :custom-eval '(ns {{namespace}}.core)})
    (System/exit 0))
  (System/exit 0))
