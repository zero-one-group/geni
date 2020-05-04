(ns zero-one.geni.ml-regression
  (:import
    (org.apache.spark.ml.regression DecisionTreeRegressor
                                    GeneralizedLinearRegression
                                    LinearRegression)))

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

(comment

  (require '[zero-one.geni.ml :as ml])
  (import '(org.apache.spark.ml.regression DecisionTreeRegressor))
  (-> (DecisionTreeRegressor.)
      ml/params)

  true)
