(ns zero-one.geni.ml-classification
  (:import
    (org.apache.spark.ml.classification DecisionTreeClassifier
                                        GBTClassifier
                                        LinearSVC
                                        LogisticRegression
                                        MultilayerPerceptronClassifier
                                        NaiveBayes
                                        OneVsRest
                                        RandomForestClassifier)))

(defn logistic-regression [{:keys [thresholds
                                   max-iter
                                   family
                                   tol
                                   raw-prediction-col
                                   elastic-net-param
                                   reg-param
                                   aggregation-depth
                                   threshold
                                   fit-intercept
                                   label-col
                                   standardization
                                   probability-col
                                   prediction-col
                                   features-col]
                            :or   {max-iter 100,
                                   family "auto",
                                   tol 1.0E-6,
                                   raw-prediction-col "rawPrediction",
                                   elastic-net-param 0.0,
                                   reg-param 0.0,
                                   aggregation-depth 2,
                                   threshold 0.5,
                                   fit-intercept true,
                                   label-col "label",
                                   standardization true,
                                   probability-col "probability",
                                   prediction-col "prediction",
                                   features-col "features"}}]
  (-> (LogisticRegression.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
      (.setMaxIter max-iter)
      (.setFamily family)
      (.setTol tol)
      (.setRawPredictionCol raw-prediction-col)
      (.setElasticNetParam elastic-net-param)
      (.setRegParam reg-param)
      (.setAggregationDepth aggregation-depth)
      (.setThreshold threshold)
      (.setFitIntercept fit-intercept)
      (.setLabelCol label-col)
      (.setStandardization standardization)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)
      (.setFeaturesCol features-col)))

(defn decision-tree-classifier
  [{:keys [thresholds
           max-bins
           min-info-gain
           impurity
           raw-prediction-col
           cache-node-ids
           seed
           label-col
           checkpoint-interval
           probability-col
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node]
    :or   {max-bins 32,
           min-info-gain 0.0,
           impurity "gini",
           raw-prediction-col "rawPrediction",
           cache-node-ids false,
           seed 159147643,
           label-col "label",
           checkpoint-interval 10,
           probability-col "probability",
           max-depth 5,
           max-memory-in-mb 256,
           prediction-col "prediction",
           features-col "features",
           min-instances-per-node 1}}]
  (-> (DecisionTreeClassifier.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
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
      (.setRawPredictionCol raw-prediction-col)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)))

(defn random-forest-classifier
  [{:keys [thresholds
           max-bins
           subsampling-rate
           min-info-gain
           impurity
           raw-prediction-col
           cache-node-ids
           seed
           label-col
           feature-subset-strategy
           checkpoint-interval
           probability-col
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node
           num-trees]
    :or   {max-bins 32,
           subsampling-rate 1.0,
           min-info-gain 0.0,
           impurity "gini",
           raw-prediction-col "rawPrediction",
           cache-node-ids false,
           seed 207336481,
           label-col "label",
           feature-subset-strategy "auto",
           checkpoint-interval 10,
           probability-col "probability",
           max-depth 5,
           max-memory-in-mb 256,
           prediction-col "prediction",
           features-col "features",
           min-instances-per-node 1,
           num-trees 20}}]
  (-> (RandomForestClassifier.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
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
      (.setRawPredictionCol raw-prediction-col)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)))

(defn gbt-classifier
  [{:keys [max-bins
           thresholds
           subsampling-rate
           max-iter
           step-size
           min-info-gain
           impurity
           raw-prediction-col
           cache-node-ids
           seed
           label-col
           feature-subset-strategy
           checkpoint-interval
           probability-col
           loss-type
           max-depth
           max-memory-in-mb
           prediction-col
           features-col
           min-instances-per-node]
    :or   {max-bins 32,
           subsampling-rate 1.0,
           max-iter 20,
           step-size 0.1,
           min-info-gain 0.0,
           impurity "gini",
           raw-prediction-col "rawPrediction",
           cache-node-ids false,
           seed -1287390502,
           label-col "label",
           feature-subset-strategy "all",
           checkpoint-interval 10,
           probability-col "probability",
           loss-type "logistic",
           max-depth 5,
           max-memory-in-mb 256,
           prediction-col "prediction",
           features-col "features",
           min-instances-per-node 1}}]
  (-> (GBTClassifier.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
      (.setMaxBins max-bins)
      (.setSubsamplingRate subsampling-rate)
      (.setMaxIter max-iter)
      (.setStepSize step-size)
      (.setMinInfoGain min-info-gain)
      (.setImpurity impurity)
      (.setCacheNodeIds cache-node-ids)
      (.setSeed seed)
      (.setLossType loss-type)
      (.setFeatureSubsetStrategy feature-subset-strategy)
      (.setCheckpointInterval checkpoint-interval)
      (.setMaxDepth max-depth)
      (.setMaxMemoryInMB max-memory-in-mb)
      (.setMinInstancesPerNode min-instances-per-node)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setRawPredictionCol raw-prediction-col)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)))

(defn mlp-classifier
  [{:keys [layers
           thresholds
           block-size
           max-iter
           step-size
           tol
           raw-prediction-col
           seed
           label-col
           probability-col
           prediction-col
           features-col
           solver]
    :or   {block-size 128,
           max-iter 100,
           step-size 0.03,
           tol 1.0E-6,
           raw-prediction-col "rawPrediction",
           seed -763139545,
           label-col "label",
           probability-col "probability",
           prediction-col "prediction",
           features-col "features",
           solver "l-bfgs"}}]
  (-> (MultilayerPerceptronClassifier.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
      (cond-> layers (.setLayers (int-array layers)))
      (.setBlockSize block-size)
      (.setMaxIter max-iter)
      (.setStepSize step-size)
      (.setTol tol)
      (.setSeed seed)
      (.setSolver solver)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setRawPredictionCol raw-prediction-col)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)))
(def multilayer-perceptron-classifier mlp-classifier)

(defn linear-svc
  [{:keys [max-iter
           tol
           raw-prediction-col
           reg-param
           aggregation-depth
           threshold
           fit-intercept
           label-col
           standardisation
           standardization
           prediction-col
           features-col]
    :or   {max-iter 100,
           tol 1.0E-6,
           raw-prediction-col "rawPrediction",
           reg-param 0.0,
           aggregation-depth 2,
           threshold 0.0,
           fit-intercept true,
           label-col "label",
           standardization true,
           prediction-col "prediction",
           features-col "features"}}]
  (let [standardisation (if (nil? standardisation)
                          standardization
                          standardisation)]
    (-> (LinearSVC.)
        (.setMaxIter max-iter)
        (.setTol tol)
        (.setRegParam reg-param)
        (.setAggregationDepth aggregation-depth)
        (.setThreshold threshold)
        (.setFitIntercept fit-intercept)
        (.setStandardization standardisation)
        (.setFeaturesCol features-col)
        (.setLabelCol label-col)
        (.setRawPredictionCol raw-prediction-col)
        (.setPredictionCol prediction-col))))

(defn one-vs-rest
  [{:keys [classifier
           weight-col
           label-col
           features-col
           parallelism
           raw-prediction-col
           prediction-col]
    :or   {label-col "label",
           features-col "features",
           parallelism 1,
           raw-prediction-col "rawPrediction",
           prediction-col "prediction"}}]
  (-> (OneVsRest.)
      (.setClassifier classifier)
      (cond-> weight-col (.setWeightCol weight-col))
      (.setParallelism parallelism)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setRawPredictionCol raw-prediction-col)
      (.setPredictionCol prediction-col)))

(defn naive-bayes
  [{:keys [thresholds
           smoothing
           prediction-col
           features-col
           raw-prediction-col
           probability-col
           label-col
           model-type]
    :or   {smoothing 1.0,
           prediction-col "prediction",
           features-col "features",
           raw-prediction-col "rawPrediction",
           probability-col "probability",
           label-col "label",
           model-type "multinomial"}}]
  (-> (NaiveBayes.)
      (cond-> thresholds (.setThresholds (double-array thresholds)))
      (.setSmoothing smoothing)
      (.setModelType model-type)
      (.setFeaturesCol features-col)
      (.setLabelCol label-col)
      (.setRawPredictionCol raw-prediction-col)
      (.setProbabilityCol probability-col)
      (.setPredictionCol prediction-col)))
