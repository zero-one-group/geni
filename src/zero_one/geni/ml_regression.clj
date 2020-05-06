(ns zero-one.geni.ml-regression
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.regression AFTSurvivalRegression
                                    DecisionTreeRegressor
                                    GBTRegressor
                                    GeneralizedLinearRegression
                                    IsotonicRegression
                                    LinearRegression
                                    RandomForestRegressor)))

(defn linear-regression
  [{:keys [max-iter
           tol
           elastic-net-param
           reg-param
           aggregation-depth
           fit-intercept
           label-col
           standardisation
           standardization
           epsilon
           loss
           prediction-col
           features-col
           solver]
    :or {max-iter 100,
         tol 1.0E-6,
         elastic-net-param 0.0,
         reg-param 0.0,
         aggregation-depth 2,
         fit-intercept true,
         label-col "label",
         standardization true,
         epsilon 1.35,
         loss "squaredError",
         prediction-col "prediction",
         features-col "features",
         solver "auto"}}]
  (let [standardisation (if (nil? standardisation)
                          standardization
                          standardisation)]
    (-> (LinearRegression.)
        (.setMaxIter max-iter)
        (.setTol tol)
        (.setElasticNetParam elastic-net-param)
        (.setRegParam reg-param)
        (.setAggregationDepth aggregation-depth)
        (.setFitIntercept fit-intercept)
        (.setStandardization standardisation)
        (.setEpsilon epsilon)
        (.setLoss loss)
        (.setSolver solver)
        (.setFeaturesCol features-col)
        (.setLabelCol label-col)
        (.setPredictionCol prediction-col))))

(defn linear-regression [params]
  (let [defaults {:max-iter 100,
                  :tol 1.0E-6,
                  :elastic-net-param 0.0,
                  :reg-param 0.0,
                  :aggregation-depth 2,
                  :fit-intercept true,
                  :label-col "label",
                  :standardization true,
                  :epsilon 1.35,
                  :loss "squaredError",
                  :prediction-col "prediction",
                  :features-col "features",
                  :solver "auto"}
        params    (-> defaults
                      (merge params))
        setters   (interop/setters-map LinearRegression)
        stage     (LinearRegression.)]
    (reduce
      (fn [_ [setter-keyword value]]
        (when-let [setter (setters setter-keyword)]
          (interop/set-value setter stage value)))
      stage
      params)))

(defn generalised-linear-regression
  [{:keys [max-iter
           variance-power
           family
           tol
           reg-param
           fit-intercept
           label-col
           prediction-col
           features-col
           solver]
    :or {max-iter 25,
         variance-power 0.0,
         family "gaussian",
         tol 1.0E-6,
         reg-param 0.0,
         fit-intercept true,
         label-col "label",
         prediction-col "prediction",
         features-col "features",
         solver "irls"}}]
  (-> (GeneralizedLinearRegression.)
      (.setMaxIter max-iter)
      (.setVariancePower variance-power)
      (.setFamily family)
      (.setTol tol)
      (.setRegParam reg-param)
      (.setFitIntercept fit-intercept)
      (.setSolver solver)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setPredictionCol prediction-col)))
(def generalized-linear-regression generalised-linear-regression)
(def glm generalised-linear-regression)

(defn decision-tree-regressor
  [{:keys [max-bins
           min-info-gain
           impurity
           cache-node-ids
           seed
           label-col
           checkpoint-interval
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node
           variance-col]
    :or {max-bins 32,
         min-info-gain 0.0,
         impurity "variance",
         cache-node-ids false,
         seed 926680331,
         label-col "label",
         checkpoint-interval 10,
         max-depth 5,
         max-memory-in-mb 256,
         prediction-col "prediction",
         features-col "features",
         min-instances-per-node 1}}]
  (-> (DecisionTreeRegressor.)
      (.setMaxBins max-bins)
      (.setMinInfoGain min-info-gain)
      (.setImpurity impurity)
      (.setCacheNodeIds cache-node-ids)
      (.setSeed seed)
      (.setCheckpointInterval checkpoint-interval)
      (.setMaxDepth max-depth)
      (.setMaxMemoryInMB max-memory-in-mb)
      (.setMinInstancesPerNode min-instances-per-node)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setPredictionCol prediction-col)
      (cond-> variance-col (.setVarianceCol variance-col))))

(defn random-forest-regressor
  [{:keys [max-bins
           subsampling-rate
           min-info-gain
           impurity
           cache-node-ids
           seed
           label-col
           feature-subset-strategy
           checkpoint-interval
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node
           num-trees]
    :or {max-bins 32,
         subsampling-rate 1.0,
         min-info-gain 0.0,
         impurity "variance",
         cache-node-ids false,
         seed 235498149,
         label-col "label",
         feature-subset-strategy "auto",
         checkpoint-interval 10,
         max-depth 5,
         max-memory-in-mb 256,
         prediction-col "prediction",
         features-col "features",
         min-instances-per-node 1,
         num-trees 20}}]
  (-> (RandomForestRegressor.)
      (.setMaxBins max-bins)
      (.setSubsamplingRate subsampling-rate)
      (.setMinInfoGain min-info-gain)
      (.setImpurity impurity)
      (.setCacheNodeIds cache-node-ids)
      (.setSeed seed)
      (.setFeatureSubsetStrategy feature-subset-strategy)
      (.setCheckpointInterval checkpoint-interval)
      (.setMaxDepth max-depth)
      (.setMaxMemoryInMB max-memory-in-mb)
      (.setMinInstancesPerNode min-instances-per-node)
      (.setNumTrees num-trees)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setPredictionCol prediction-col)))


(defn gbt-regressor
  [{:keys [max-bins
           subsampling-rate
           max-iter
           step-size
           min-info-gain
           impurity
           cache-node-ids
           seed
           label-col
           feature-subset-strategy
           checkpoint-interval
           loss-type
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node]
    :or {max-bins 32,
         subsampling-rate 1.0,
         max-iter 20,
         step-size 0.1,
         min-info-gain 0.0,
         impurity "variance",
         cache-node-ids false,
         seed -131597770,
         label-col "label",
         feature-subset-strategy "all",
         checkpoint-interval 10,
         loss-type "squared",
         max-depth 5,
         max-memory-in-mb 256,
         prediction-col "prediction",
         features-col "features",
         min-instances-per-node 1}}]
  (-> (GBTRegressor.)
      (.setMaxBins max-bins)
      (.setSubsamplingRate subsampling-rate)
      (.setMaxIter max-iter)
      (.setStepSize step-size)
      (.setMinInfoGain min-info-gain)
      (.setImpurity impurity)
      (.setCacheNodeIds cache-node-ids)
      (.setSeed seed)
      (.setFeatureSubsetStrategy feature-subset-strategy)
      (.setCheckpointInterval checkpoint-interval)
      (.setLossType loss-type)
      (.setMaxDepth max-depth)
      (.setMaxMemoryInMB max-memory-in-mb)
      (.setMinInstancesPerNode min-instances-per-node)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setPredictionCol prediction-col)))

(defn aft-survival-regression
  [{:keys [max-iter
           tol
           quantile-probabilities
           aggregation-depth
           fit-intercept
           label-col
           censor-col
           prediction-col
           features-col]
    :or {max-iter 100,
         tol 1.0E-6,
         quantile-probabilities [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
         aggregation-depth 2,
         fit-intercept true,
         label-col "label",
         censor-col "censor",
         prediction-col "prediction",
         features-col "features"}}]
  (-> (AFTSurvivalRegression.)
      (.setMaxIter max-iter)
      (.setTol tol)
      (.setQuantileProbabilities (double-array quantile-probabilities))
      (.setAggregationDepth aggregation-depth)
      (.setFitIntercept fit-intercept)
      (.setLabelCol label-col)
      (.setCensorCol censor-col)
      (.setPredictionCol prediction-col)
      (.setFeaturesCol features-col)))

(defn isotonic-regression
  [{:keys [prediction-col
           features-col
           isotonic
           label-col
           feature-index]
    :or   {prediction-col "prediction",
           features-col "features",
           isotonic true,
           label-col "label",
           feature-index 0}}]
  (-> (IsotonicRegression.)
      (.setIsotonic isotonic)
      (.setFeatureIndex feature-index)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setPredictionCol prediction-col)))

(comment

  (ns user)
  (require '[zero-one.geni.ml :as ml])

  (import '(org.apache.spark.ml.regression RandomForestRegressor))
  (-> (RandomForestRegressor.)
      ml/params)

  true)
