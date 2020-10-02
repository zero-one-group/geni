(ns scripts.scrape-spark-docs
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [net.cgrand.enlive-html :as html]
    [taoensso.nippy :as nippy]))

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

(defn url->method-docs [url]
  (Thread/sleep 100)
  (->> (html/html-resource (java.net.URL. url))
       fn-candidate-nodes
       (filter (every-pred has-name? has-result?))
       (map #(vector (->kebab-case (extract-name %))
                     (format "Params: %s\nResult%s\n%s\nSource: %s"
                             (extract-params %)
                             (extract-result %)
                             (extract-comment %)
                             url)))
       (into {})))

(def spark-doc-url
  "https://spark.apache.org/docs/latest/api/scala/org/apache/spark/")

(def method-doc-url-map
  {:core {:column    "sql/Column.html"
          :dataset   "sql/Dataset.html"
          :functions "sql/functions$.html"
          :grouped   "sql/RelationalGroupedDataset.html"
          :na-fns    "sql/DataFrameNaFunctions.html"
          :stat-fns  "sql/DataFrameStatFunctions.html"
          :window    "sql/expressions/Window.html"}
   :ml {}
   :rdd {:rdd      "api/java/JavaRDD.html"
         :pair-rdd "api/java/JavaPairRDD.html"}
   :spark {:session "sql/SparkSession.html"
           :context "api/java/JavaSparkContext.html"}
   :streaming {:context      "streaming/api/java/JavaStreamingContext.html"
               :dstream      "streaming/api/java/JavaDStream.html"
               :pair-dstream "streaming/api/java/JavaPairDStream.html"}})

(defn walk-doc-map [url->map package-map]
  (->> package-map
       (map (fn [[path-key map-or-url]]
              (vector path-key
                      (if (map? map-or-url)
                        (walk-doc-map url->map map-or-url)
                        (url->map (str spark-doc-url map-or-url))))))
       (into {})))

;; TODO: url->class-doc
;; TODO: class-doc-url-map
;; TODO: (def class-docs (walk-doc-map url->class-doc class-doc-url-map))

(comment

  (def method-docs (walk-doc-map url->method-docs method-doc-url-map))
  (-> method-docs :spark :session (#(% "to-string")) print)

  (def complete-docs (merge method-docs {}))

  (nippy/freeze-to-file
    "target/docs.nippy"
    complete-docs
    {:compressor nippy/lz4hc-compressor})

  (def thawed
    (nippy/thaw-from-file "target/docs.nippy"))

  (-> thawed :spark :session (#(% "to-string")) println)

  true)

;; TODO: what about storage level?
;; TODO: find a way to attach doc-maps to Geni fns.
