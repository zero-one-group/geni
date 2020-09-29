(ns scripts.scrape-spark-docs
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [net.cgrand.enlive-html :as html]))

(defn fn-candidate-nodes [initial-node]
  (html/select initial-node [:li]))

(defn has-content? [node selector]
  (-> node (html/select selector) first :content some?))

(defn has-name? [node]
  (has-content? node [:span.symbol :span.name]))

(defn has-result? [node]
  (has-content? node [:span.symbol :span.result]))

(defn extract-text [node selector]
  (-> node (html/select selector) first html/text (or "")))

(defn extract-name [node]
  (extract-text node [:span.symbol :span.name]))

(defn extract-params [node]
  (extract-text node [:span.symbol :span.params]))

(defn extract-result [node]
  (extract-text node [:span.symbol :span.result]))

(defn extract-comment [node]
  (extract-text node [:div.fullcomment :p]))

(defn ->doc-map [initial-node]
  (->> (fn-candidate-nodes initial-node)
       (filter (every-pred has-name? has-result?))
       (map #(vector (->kebab-case (extract-name %))
                     (format "Params: %s\nResult%s\n%s\n"
                             (extract-params %)
                             (extract-result %)
                             (extract-comment %))))
       (into {})))

(def spark-doc-url
  "https://spark.apache.org/docs/latest/api/scala/org/apache/spark/")

(def doc-url
  ;(str spark-doc-url "sql/functions$.html")
  (str spark-doc-url "streaming/api/java/JavaStreamingContext.html"))

(def parent-node
  (html/html-resource (java.net.URL. doc-url)))

(defn ->html-resource [url]
  (html/html-resource (java.net.URL. url)))

(def spark-package-map
  {:core {:column    "sql/Column.html"
          :dataset   "sql/Dataset.html"
          :functions "sql/functions$.html"
          :na-fns    "sql/DataFrameNaFunctions.html"
          :stat-fns  "sql/DataFrameStatFunctions.html"
          :window    "sql/expressions/Window.html"}
   :ml {}
   :rdd {}
   :streaming {}})

(defn walk-doc-map [package-map]
  (->> package-map
       (map (fn [[k v]]
              [k (if (map? v)
                   (walk-doc-map v)
                   (-> (str spark-doc-url v)
                       ->html-resource
                       ->doc-map))]))
       (into {})))


(def complete-doc-map (walk-doc-map spark-package-map))

(-> complete-doc-map :core :na-fns keys sort)

;; TODO: go through all the Spark namespaces that matter.
;; TODO: create a nested map of {namespace {fn-name comment}}.
;; TODO: find a way to attach it to Geni fns.
