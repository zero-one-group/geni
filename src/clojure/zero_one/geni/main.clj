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
  (future
    (.start streaming-context))
  (future
    (.awaitTermination streaming-context))
  (.stop streaming-context true true)


  (require '[zero-one.geni.test-resources :refer [melbourne-df]])
  (def dataframe melbourne-df)
  (-> dataframe g/count)
  (-> dataframe g/print-schema)

  (require '[midje.repl :refer [autotest]])
  (autotest :filter (every-pred :streaming (complement :slow)))
  ;(autotest :filter (every-pred (complement :slow) (complement :repl)))

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (import '(org.apache.spark.api.java JavaPairRDD))
  (->> (r/reflect Long)
       ;:members
       ;(filter #(= (:name %) 'socketTextStream))
       ;(mapv :name)
       ;sort
       clojure.pprint/pprint)


  (require '[net.cgrand.enlive-html :as html])
  (require '[camel-snake-kebab.core :refer [->kebab-case]])

  (def url-prefix
    "https://spark.apache.org/docs/latest/api/scala/org/apache/spark/")

  (def url
    ;(str url-prefix "sql/functions$.html")
    (str url-prefix "streaming/api/java/JavaStreamingContext.html"))

  (def parent-node
    (html/html-resource (java.net.URL. url)))


  ;; TODO: extract all fn names
  (def fn-candidate-nodes
    (html/select parent-node [:li]))

  (defn extract-content [node selector]
    (-> node (html/select selector) first :content))

  (defn has-name? [node]
    (extract-content node [:span.symbol :span.name]))

  (defn has-result? [node]
    (extract-content node [:span.symbol :span.result]))

  (defn extract-name [node]
    (-> node (html/select [:span.symbol :span.name]) first html/text))

  (defn extract-params [node]
    (-> node (html/select [:span.symbol :span.params]) first html/text))

  (defn extract-result [node]
    (-> node (html/select [:span.symbol :span.result]) first html/text))

  (defn extract-comment [node]
    (-> node (html/select [:div.fullcomment :p]) first html/text))

  (->> fn-candidate-nodes
       (filter (every-pred has-name? has-result?))
       (map #(vector (->kebab-case (extract-name %))
                     (str (extract-params %)
                          (extract-result %)
                          "\n"
                          (extract-comment %))))
       (into {}))

  ;; TODO: go through all the Spark namespaces that matter.
  ;; TODO: create a nested map of {namespace {fn-name comment}}.
  ;; TODO: find a way to attach it to Geni fns.

  0)
