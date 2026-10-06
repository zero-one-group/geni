(ns zero-one.geni.ml.summary
  "Spark ML's model summaries as Clojure maps, from a fixed list of each
  summary's values, by the classes and traits it extends."
  (:import
   (clojure.lang Reflector)
   (org.apache.spark.ml Model)
   (org.apache.spark.ml.classification BinaryClassificationSummary
                                       ClassificationSummary
                                       LogisticRegressionSummary
                                       TrainingSummary)
   (org.apache.spark.ml.clustering BisectingKMeansSummary
                                   ClusteringSummary
                                   GaussianMixtureSummary
                                   KMeansSummary)
   (org.apache.spark.ml.linalg Vector)
   (org.apache.spark.ml.regression GeneralizedLinearRegressionSummary
                                   GeneralizedLinearRegressionTrainingSummary
                                   LinearRegressionSummary
                                   LinearRegressionTrainingSummary)
   (org.apache.spark.sql Dataset)
   (scala Product)))

(def ^:private summary-values
  "Each summary class or trait, and the values that a summary of it has, as
  keys and the methods that give them."
  [[ClassificationSummary
    [[:predictions "predictions"]
     [:prediction-col "predictionCol"]
     [:label-col "labelCol"]
     [:weight-col "weightCol"]
     [:labels "labels"]
     [:accuracy "accuracy"]
     [:true-positive-rate-by-label "truePositiveRateByLabel"]
     [:false-positive-rate-by-label "falsePositiveRateByLabel"]
     [:precision-by-label "precisionByLabel"]
     [:recall-by-label "recallByLabel"]
     [:f-measure-by-label "fMeasureByLabel"]
     [:weighted-true-positive-rate "weightedTruePositiveRate"]
     [:weighted-false-positive-rate "weightedFalsePositiveRate"]
     [:weighted-recall "weightedRecall"]
     [:weighted-precision "weightedPrecision"]
     [:weighted-f-measure "weightedFMeasure"]]]
   [TrainingSummary
    [[:objective-history "objectiveHistory"]
     [:total-iterations "totalIterations"]]]
   [BinaryClassificationSummary
    [[:score-col "scoreCol"]
     [:area-under-roc "areaUnderROC"]
     [:roc "roc"]
     [:pr "pr"]
     [:f-measure-by-threshold "fMeasureByThreshold"]
     [:precision-by-threshold "precisionByThreshold"]
     [:recall-by-threshold "recallByThreshold"]]]
   [LogisticRegressionSummary
    [[:probability-col "probabilityCol"]
     [:features-col "featuresCol"]]]
   [ClusteringSummary
    [[:predictions "predictions"]
     [:prediction-col "predictionCol"]
     [:features-col "featuresCol"]
     [:k "k"]
     [:num-iter "numIter"]
     [:cluster "cluster"]
     [:cluster-sizes "clusterSizes"]]]
   [KMeansSummary
    [[:training-cost "trainingCost"]]]
   [BisectingKMeansSummary
    [[:training-cost "trainingCost"]]]
   [GaussianMixtureSummary
    [[:probability-col "probabilityCol"]
     [:probability "probability"]
     [:log-likelihood "logLikelihood"]]]
   [LinearRegressionSummary
    [[:predictions "predictions"]
     [:prediction-col "predictionCol"]
     [:label-col "labelCol"]
     [:features-col "featuresCol"]
     [:explained-variance "explainedVariance"]
     [:mean-absolute-error "meanAbsoluteError"]
     [:mean-squared-error "meanSquaredError"]
     [:root-mean-squared-error "rootMeanSquaredError"]
     [:r2 "r2"]
     [:r2adj "r2adj"]
     [:residuals "residuals"]
     [:num-instances "numInstances"]
     [:degrees-of-freedom "degreesOfFreedom"]
     [:deviance-residuals "devianceResiduals"]
     [:coefficient-standard-errors "coefficientStandardErrors"]
     [:t-values "tValues"]
     [:p-values "pValues"]]]
   [LinearRegressionTrainingSummary
    [[:objective-history "objectiveHistory"]
     [:total-iterations "totalIterations"]]]
   [GeneralizedLinearRegressionSummary
    [[:predictions "predictions"]
     [:prediction-col "predictionCol"]
     [:rank "rank"]
     [:num-instances "numInstances"]
     [:degrees-of-freedom "degreesOfFreedom"]
     [:residual-degree-of-freedom "residualDegreeOfFreedom"]
     [:residual-degree-of-freedom-null "residualDegreeOfFreedomNull"]
     [:residuals "residuals"]
     [:null-deviance "nullDeviance"]
     [:deviance "deviance"]
     [:dispersion "dispersion"]
     [:aic "aic"]]]
   [GeneralizedLinearRegressionTrainingSummary
    [[:num-iterations "numIterations"]
     [:solver "solver"]
     [:coefficient-standard-errors "coefficientStandardErrors"]
     [:t-values "tValues"]
     [:p-values "pValues"]
     [:coefficients-with-statistics "coefficientsWithStatistics"]]]])

(defn- ->value
  "A summary's value as Clojure data: an array or an MLlib vector as a
  vector, and a generalised linear regression's coefficient statistics as
  maps. DataFrames stay DataFrames."
  [x]
  (cond
    (instance? Dataset x) x
    (instance? Vector x)  (vec (.toArray ^Vector x))
    (instance? Product x) (let [^Product p x
                                [feature estimate std-error t-value p-value]
                                (map #(.productElement p (int %)) (range (.productArity p)))]
                            {:feature   feature
                             :estimate  estimate
                             :std-error std-error
                             :t-value   t-value
                             :p-value   p-value})
    (and (some? x) (.isArray (class x))) (mapv ->value x)
    :else x))

(def ^:private unavailable
  "What a value that Spark can't give for this model is: its standard
  errors without the normal solver, say, which Spark refuses, or its
  p-values with no residual degrees of freedom, as a fit of more features
  than rows has, which Spark's `require` refuses."
  ::unavailable)

(defn- value-of [summary method]
  (try
    (->value (Reflector/invokeNoArgInstanceMember summary method false))
    (catch UnsupportedOperationException _ unavailable)
    (catch IllegalArgumentException e
      (if (some-> (ex-message e) (.startsWith "requirement failed"))
        unavailable
        (throw e)))))

(defn summary->map
  "A summary's values as a map, for the summary classes that `summary-values`
  lists, leaving out what Spark can't give for the model."
  [summary]
  (let [entries (for [[^Class cls values] summary-values
                      :when (instance? cls summary)
                      [k method] values]
                  [k method])]
    (when (empty? entries)
      (throw (ex-info (str "ml/summary takes a model with a training summary, or a summary, "
                           "and not a " (.getName (class summary)) ".")
                      {:class (class summary)})))
    (into {}
          (keep (fn [[k method]]
                  (let [v (value-of summary method)]
                    (when-not (identical? unavailable v) [k v]))))
          (distinct entries))))

(defn- model-summary
  "The summary that `model`'s no-argument method `method` gives, or an error
  that names the model when it has none."
  [model method]
  (if (some #(and (= method (.getName ^java.lang.reflect.Method %))
                  (zero? (.getParameterCount ^java.lang.reflect.Method %)))
            (.getMethods (class model)))
    (Reflector/invokeNoArgInstanceMember model method false)
    (throw (ex-info (str (.getSimpleName (class model)) " has no " method ".")
                    {:class (class model)}))))

(defn summary
  "A model's training summary, or a summary that `evaluate` gives for a
  model and new data, as a map of its values: for a classifier, its accuracy,
  the rates, precision and recall by label and weighted, and for a binary
  one, the area under the ROC curve; for a regression, its errors and r2, and
  for a linear or generalised linear one, the coefficients' statistics; for a
  clustering model, its cluster sizes and cost; and the objective's history
  where the model has one. ROC and PR curves, residuals and predictions stay
  DataFrames. A value that Spark can't give for the model, such as a linear
  regression's standard errors without the normal solver, or its p-values for
  more features than rows, is left out.

  Spark computes most of the metrics from the predictions, so the map takes
  a job or two to make. `(.summary model)` gives Spark's own summary.

  ```clojure
  (:area-under-roc (ml/summary model))
  (ml/summary (ml/evaluate test-data model))
  ```"
  [model-or-summary]
  (summary->map (if (instance? Model model-or-summary)
                  (model-summary model-or-summary "summary")
                  model-or-summary)))

(defn binary-summary
  "A logistic regression model's training summary as `summary` gives it, with
  the binary classifier's values, which throws for a multinomial model, or a
  binary summary as a map."
  [model-or-summary]
  (summary->map (if (instance? Model model-or-summary)
                  (model-summary model-or-summary "binarySummary")
                  model-or-summary)))
