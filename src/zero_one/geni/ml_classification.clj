(ns zero-one.geni.ml-classification
  (:require
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [coalesce]])
  (:import
    (org.apache.spark.ml.classification DecisionTreeClassifier
                                        GBTClassifier
                                        LinearSVC
                                        LogisticRegression
                                        MultilayerPerceptronClassifier
                                        NaiveBayes
                                        OneVsRest
                                        RandomForestClassifier)))

(defn logistic-regression [params]
  (let [defaults {:max-iter           100,
                  :family             "auto",
                  :tol                1.0E-6,
                  :raw-prediction-col "rawPrediction",
                  :elastic-net-param  0.0,
                  :reg-param          0.0,
                  :aggregation-depth  2,
                  :threshold          0.5,
                  :fit-intercept      true,
                  :label-col          "label",
                  :standardization    true,
                  :probability-col    "probability",
                  :prediction-col     "prediction",
                  :features-col       "features"}
        std       (coalesce (:standardisation params)
                            (:standardization params)
                            (:standardization defaults))
        props     (-> defaults
                      (merge params)
                      (assoc :standardization std))]
    (interop/instantiate LogisticRegression props)))

(defn decision-tree-classifier [params]
  (let [defaults {:max-bins               32,
                  :min-info-gain          0.0,
                  :impurity               "gini",
                  :raw-prediction-col     "rawPrediction",
                  :cache-node-ids         false,
                  :seed                   159147643,
                  :label-col              "label",
                  :checkpoint-interval    10,
                  :probability-col        "probability",
                  :max-depth              5,
                  :max-memory-in-mb       256,
                  :prediction-col         "prediction",
                  :features-col           "features",
                  :min-instances-per-node 1}
        props     (merge defaults params)]
    (interop/instantiate DecisionTreeClassifier props)))

(defn random-forest-classifier [params]
  (let [defaults {:max-bins 32,
                  :subsampling-rate 1.0,
                  :min-info-gain 0.0,
                  :impurity "gini",
                  :raw-prediction-col "rawPrediction",
                  :cache-node-ids false,
                  :seed 207336481,
                  :label-col "label",
                  :feature-subset-strategy "auto",
                  :checkpoint-interval 10,
                  :probability-col "probability",
                  :max-depth 5,
                  :max-memory-in-mb 256,
                  :prediction-col "prediction",
                  :features-col "features",
                  :min-instances-per-node 1,
                  :num-trees 20}
        props     (merge defaults params)]
    (interop/instantiate RandomForestClassifier props)))

(defn gbt-classifier [params]
  (let [defaults {:max-bins                32,
                  :subsampling-rate        1.0,
                  :max-iter                20,
                  :step-size               0.1,
                  :min-info-gain           0.0,
                  :impurity                "gini",
                  :raw-prediction-col      "rawPrediction",
                  :cache-node-ids          false,
                  :seed                    -1287390502,
                  :label-col               "label",
                  :feature-subset-strategy "all",
                  :checkpoint-interval     10,
                  :probability-col         "probability",
                  :loss-type               "logistic",
                  :max-depth               5,
                  :max-memory-in-mb        256,
                  :prediction-col          "prediction",
                  :features-col            "features",
                  :min-instances-per-node  1}
        props     (merge defaults params)]
    (interop/instantiate GBTClassifier props)))

(defn mlp-classifier [params]
  (let [defaults {:block-size         128,
                  :max-iter           100,
                  :step-size          0.03,
                  :tol                1.0E-6,
                  :raw-prediction-col "rawPrediction",
                  :seed               -763139545,
                  :label-col          "label",
                  :probability-col    "probability",
                  :prediction-col     "prediction",
                  :features-col       "features",
                  :solver             "l-bfgs"}
        props    (merge defaults params)]
    (interop/instantiate MultilayerPerceptronClassifier props)))
(def multilayer-perceptron-classifier)

(defn linear-svc [params]
  (let [defaults {:max-iter           100,
                  :tol                1.0E-6,
                  :raw-prediction-col "rawPrediction",
                  :reg-param          0.0,
                  :aggregation-depth  2,
                  :threshold          0.0,
                  :fit-intercept      true,
                  :label-col          "label",
                  :standardization    true,
                  :prediction-col     "prediction",
                  :features-col       "features"}
        std       (coalesce (:standardisation params)
                            (:standardization params)
                            (:standardization defaults))
        props     (-> defaults
                      (merge params)
                      (assoc :standardization std))]
    (interop/instantiate LinearSVC props)))

(defn one-vs-rest [params]
  (let [defaults {:label-col          "label",
                  :features-col       "features",
                  :parallelism        1,
                  :raw-prediction-col "rawPrediction",
                  :prediction-col     "prediction"}
        props    (merge defaults params)]
    (interop/instantiate OneVsRest props)))

(defn naive-bayes [params]
  (let [defaults {:smoothing 1.0,
                  :prediction-col "prediction",
                  :features-col "features",
                  :raw-prediction-col "rawPrediction",
                  :probability-col "probability",
                  :label-col "label",
                  :model-type "multinomial"}
        props    (merge defaults params)]
    (interop/instantiate NaiveBayes props)))
