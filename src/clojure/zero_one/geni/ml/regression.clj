(ns zero-one.geni.ml.regression
  (:require
    [zero-one.geni.docs :refer [spark-docs]]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [coalesce]])
  (:import
    (org.apache.spark.ml.regression AFTSurvivalRegression
                                    DecisionTreeRegressor
                                    FMRegressor
                                    GBTRegressor
                                    GeneralizedLinearRegression
                                    IsotonicRegression
                                    LinearRegression
                                    RandomForestRegressor)))

(defn linear-regression
  {:doc (-> spark-docs :ml :regression :linear-regression)}
  [params]
  (let [defaults {:max-iter          100,
                  :tol               1.0E-6,
                  :elastic-net-param 0.0,
                  :reg-param         0.0,
                  :aggregation-depth 2,
                  :fit-intercept     true,
                  :label-col         "label",
                  :standardization   true,
                  :epsilon           1.35,
                  :loss              "squaredError",
                  :prediction-col    "prediction",
                  :features-col      "features",
                  :solver            "auto"}
        std       (coalesce (:standardisation params)
                            (:standardization params)
                            (:standardization defaults))
        props     (-> defaults
                      (merge params)
                      (assoc :standardization std))]
    (interop/instantiate LinearRegression props)))

(defn generalised-linear-regression
  {:doc (-> spark-docs :ml :regression :generalized-linear-regression)}
  [params]
  (let [defaults {:max-iter       25,
                  :variance-power 0.0,
                  :family         "gaussian",
                  :tol            1.0E-6,
                  :reg-param      0.0,
                  :fit-intercept  true,
                  :label-col      "label",
                  :prediction-col "prediction",
                  :features-col   "features",
                  :solver         "irls"}
        props     (merge defaults params)]
    (interop/instantiate GeneralizedLinearRegression props)))
(def generalized-linear-regression
  generalised-linear-regression)
(def glm generalised-linear-regression)

(defn decision-tree-regressor
  {:doc (-> spark-docs :ml :regression :decision-tree-regressor)}
  [params]
  (let [defaults {:max-bins                     32,
                  :min-info-gain                0.0,
                  :impurity                     "variance",
                  :cache-node-ids               false,
                  :seed                         926680331,
                  :label-col                    "label",
                  :leaf-col                     ""
                  :checkpoint-interval          10,
                  :min-weight-fraction-per-node 0.0,
                  :max-depth                    5,
                  :max-memory-in-mb             256,
                  :prediction-col               "prediction",
                  :features-col                 "features",
                  :min-instances-per-node       1}
        props     (merge defaults params)]
    (interop/instantiate DecisionTreeRegressor props)))

(defn random-forest-regressor
  {:doc (-> spark-docs :ml :regression :random-forest-regressor)}
  [params]
  (let [defaults {:max-bins                     32,
                  :subsampling-rate             1.0,
                  :min-info-gain                0.0,
                  :impurity                     "variance",
                  :min-weight-fraction-per-node 0.0,
                  :cache-node-ids               false,
                  :seed                         235498149,
                  :label-col                    "label",
                  :leaf-col                     ""
                  :feature-subset-strategy      "auto",
                  :checkpoint-interval          10,
                  :max-depth                    5,
                  :max-memory-in-mb             256,
                  :prediction-col               "prediction",
                  :features-col                 "features",
                  :min-instances-per-node       1,
                  :num-trees                    20}
        props     (merge defaults params)]
    (interop/instantiate RandomForestRegressor props)))

(defn gbt-regressor
  {:doc (-> spark-docs :ml :regression :gbt-regressor)}
  [params]
  (let [defaults {:max-bins                     32,
                  :subsampling-rate             1.0,
                  :max-iter                     20,
                  :step-size                    0.1,
                  :min-info-gain                0.0,
                  :cache-node-ids               false,
                  :seed                         -131597770,
                  :label-col                    "label",
                  :leaf-col                     ""
                  :min-weight-fraction-per-node 0.0,
                  :feature-subset-strategy      "all",
                  :checkpoint-interval          10,
                  :loss-type                    "squared",
                  :max-depth                    5,
                  :max-memory-in-mb             256,
                  :prediction-col               "prediction",
                  :features-col                 "features",
                  :min-instances-per-node       1}
        props     (merge defaults params)]
    (interop/instantiate GBTRegressor props)))

(defn aft-survival-regression
  {:doc (-> spark-docs :ml :regression :aft-survival-regression)}
  [params]
  (let [q-probs  [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        defaults {:max-iter               100,
                  :tol                    1.0E-6,
                  :quantile-probabilities q-probs,
                  :aggregation-depth      2,
                  :fit-intercept          true,
                  :label-col              "label",
                  :censor-col             "censor",
                  :prediction-col         "prediction",
                  :features-col           "features"}
        props     (-> (merge defaults params))]
    (interop/instantiate AFTSurvivalRegression props)))

(defn isotonic-regression
  {:doc (-> spark-docs :ml :regression :isotonic-regression)}
  [params]
  (let [defaults {:prediction-col "prediction",
                  :features-col   "features",
                  :isotonic       true,
                  :label-col      "label",
                  :feature-index  0}
        props     (-> (merge defaults params))]
    (interop/instantiate IsotonicRegression props)))

(defn fm-regressor
  {:doc (-> spark-docs :ml :regression :fm-regressor)}
  [params]
  (let [defaults {:max-iter            100,
                  :step-size           1.0,
                  :tol                 1.0E-6,
                  :reg-param           0.0,
                  :seed                891375198,
                  :mini-batch-fraction 1.0,
                  :fit-intercept       true,
                  :label-col           "label",
                  :factor-size         8,
                  :fit-linear          true,
                  :prediction-col      "prediction",
                  :init-std            0.01,
                  :features-col        "features",
                  :solver              "adamW"}
        props     (-> (merge defaults params))]
    (interop/instantiate FMRegressor props)))

(defn f
  "abc"
  [x]
  x)

(def g f)

(-> f var meta :doc) ; => "abc"
(-> g var meta :doc) ; => nil

(def ^{:doc (-> f var meta :doc)} g f)
(-> g var meta :doc) ; => "abc"
