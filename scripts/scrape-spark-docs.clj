(ns scripts.scrape-spark-docs
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [net.cgrand.enlive-html :as html]
    [taoensso.nippy :as nippy]
    [zero-one.geni.core :as g]))

(def spark-version (g/version))

(def spark-doc-url
  (format "https://spark.apache.org/docs/%s/api/scala/org/apache/spark/"
          spark-version))

(defn timestamp! []
  (-> (java.util.Date.)
      .toInstant
      .toString))

(defn polite-html-resource [url]
  (Thread/sleep 100)
  (html/html-resource (java.net.URL. url)))

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
  (->> (polite-html-resource url)
       fn-candidate-nodes
       (filter (every-pred has-name? has-result?))
       (map #(vector (keyword (->kebab-case (extract-name %)))
                     (format "Params: %s\nResult%s\n%s\nSource: %s\nTimestamp: %s"
                             (extract-params %)
                             (extract-result %)
                             (extract-comment %)
                             url
                             (timestamp!))))
       (into {})))

(defn extract-title [node]
  (extract-text node [:h1 :a]))

(defn extract-class-comment [node]
  (extract-text node [:div#comment :div.comment.cmt]))

(defn url->class-docs [url]
  (let [resource  (polite-html-resource url)
        fn-name   (->kebab-case (extract-title resource))
        class-doc (format "%s\nSource: %s\nTimestamp: %s"
                          (extract-class-comment resource)
                          url
                          (timestamp!))]
    {(keyword fn-name) class-doc}))

(def class-doc-url-map
  {:ml {:classification ["ml/classification/DecisionTreeClassifier.html"
                         "ml/classification/FMClassifier.html"
                         "ml/classification/GBTClassifier.html"
                         "ml/classification/LinearSVC.html"
                         "ml/classification/LogisticRegression.html"
                         "ml/classification/MultilayerPerceptronClassifier.html"
                         "ml/classification/NaiveBayes.html"
                         "ml/classification/OneVsRest.html"
                         "ml/classification/RandomForestClassifier.html"]
        :clustering ["ml/clustering/BisectingKMeans.html"
                     "ml/clustering/GaussianMixture.html"
                     "ml/clustering/KMeans.html"
                     "ml/clustering/LDA.html"
                     "ml/clustering/PowerIterationClustering.html"]
        :tuning ["ml/tuning/CrossValidator.html"
                 "ml/tuning/TrainValidationSplit.html"]
        :evaluation ["ml/evaluation/BinaryClassificationEvaluator.html"
                     "ml/evaluation/ClusteringEvaluator.html"
                     "ml/evaluation/MulticlassClassificationEvaluator.html"
                     "ml/evaluation/MultilabelClassificationEvaluator.html"
                     "ml/evaluation/RankingEvaluator.html"
                     "ml/evaluation/RegressionEvaluator.html"]
        :regression ["ml/regression/AFTSurvivalRegression.html"
                     "ml/regression/DecisionTreeRegressor.html"
                     "ml/regression/FMRegressor.html"
                     "ml/regression/GBTRegressor.html"
                     "ml/regression/GeneralizedLinearRegression.html"
                     "ml/regression/IsotonicRegression.html"
                     "ml/regression/LinearRegression.html"
                     "ml/regression/RandomForestRegressor.html"]
        :recommendation ["ml/recommendation/ALS.html"]
        :feature ["ml/feature/Binarizer.html"
                  "ml/feature/BucketedRandomProjectionLSH.html"
                  "ml/feature/Bucketizer.html"
                  "ml/feature/ChiSqSelector.html"
                  "ml/feature/CountVectorizer.html"
                  "ml/feature/DCT.html"
                  "ml/feature/ElementwiseProduct.html"
                  "ml/feature/FeatureHasher.html"
                  "ml/feature/HashingTF.html"
                  "ml/feature/IDF.html"
                  "ml/feature/Imputer.html"
                  "ml/feature/IndexToString.html"
                  "ml/feature/Interaction.html"
                  "ml/feature/LabeledPoint.html"
                  "ml/feature/MaxAbsScaler.html"
                  "ml/feature/MinHashLSH.html"
                  "ml/feature/MinMaxScaler.html"
                  "ml/feature/NGram.html"
                  "ml/feature/Normalizer.html"
                  "ml/feature/OneHotEncoder.html"
                  "ml/feature/PCA.html"
                  "ml/feature/PolynomialExpansion.html"
                  "ml/feature/QuantileDiscretizer.html"
                  "ml/feature/RegexTokenizer.html"
                  "ml/feature/RFormula.html"
                  "ml/feature/RobustScaler.html"
                  "ml/feature/SQLTransformer.html"
                  "ml/feature/StandardScaler.html"
                  "ml/feature/StopWordsRemover.html"
                  "ml/feature/StringIndexer.html"
                  "ml/feature/Tokenizer.html"
                  "ml/feature/VectorAssembler.html"
                  "ml/feature/VectorIndexer.html"
                  "ml/feature/VectorSizeHint.html"
                  "ml/feature/VectorSlicer.html"
                  "ml/feature/Word2Vec.html"]}})

(def method-doc-url-map
  {:core {:column    "sql/Column.html"
          :dataset   "sql/Dataset.html"
          :functions "sql/functions$.html"
          :grouped   "sql/RelationalGroupedDataset.html"
          :na-fns    "sql/DataFrameNaFunctions.html"
          :stat-fns  "sql/DataFrameStatFunctions.html"
          :window    "sql/expressions/Window.html"}
   :rdd {:rdd      "api/java/JavaRDD.html"
         :pair-rdd "api/java/JavaPairRDD.html"}
   :spark {:session "sql/SparkSession.html"
           :context "api/java/JavaSparkContext.html"}
   :streaming {:context      "streaming/api/java/JavaStreamingContext.html"
               :dstream      "streaming/api/java/JavaDStream.html"
               :pair-dstream "streaming/api/java/JavaPairDStream.html"}})

(defn walk-doc-map [url->map package-map]
  (letfn [(prefix-url [url] (str spark-doc-url url))]
    (->> package-map
         (map (fn [[path-key url-node]]
                (vector
                  path-key
                  (cond
                    (map? url-node)  (walk-doc-map url->map url-node)
                    (coll? url-node) (->> url-node
                                          (map (comp url->map prefix-url))
                                          (apply merge))
                    :else            (url->map (prefix-url url-node))))))
         (into {}))))

(defn scrape-spark-docs! []
  (let [class-docs    (walk-doc-map url->class-docs class-doc-url-map)
        method-docs   (walk-doc-map url->method-docs method-doc-url-map)
        complete-docs (merge method-docs class-docs)]
    (nippy/freeze-to-file
      "resources/spark-docs.nippy"
      complete-docs
      {:compressor nippy/lz4hc-compressor})))

(comment

  (ns-publics 'scripts.scrape-spark-docs)

  (scrape-spark-docs!)

  (def spark-docs
    (nippy/thaw-from-file "resources/spark-docs.nippy"))

  (-> spark-docs :ml :regression :fm-regressor (or "") println)

  (-> spark-docs :ml :regression keys)

  true)
