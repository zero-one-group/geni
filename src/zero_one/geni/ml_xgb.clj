(ns zero-one.geni.ml-xgb)

(declare xgboost-classifier
         xgboost-regressor)

(defmacro include-xgboost-fns [throw?]
  (if (try
        (when throw?
          (throw (ClassNotFoundException. "Intentional exception.")))
        (require
          '[zero-one.geni.interop :as interop]
          '[zero-one.geni.utils :refer [coalesce]])
        (import
          '(ml.dmlc.xgboost4j.scala.spark XGBoostClassifier
                                          XGBoostRegressor))
        (catch ClassNotFoundException _ nil))

   '(do
      (defn xgboost-classifier [params]
          (let [defaults  {:num-round 1,
                           :dmlc-worker-connect-retry 5,
                           :subsample 1.0,
                           :num-early-stopping-rounds 0,
                           :cache-training-set false,
                           :allow-non-zero-for-missing false,
                           :checkpoint-path "",
                           :verbosity 1,
                           :scale-pos-weight 1.0,
                           :raw-prediction-col "rawPrediction",
                           :lambda 1.0,
                           :silent 0,
                           :num-workers 1,
                           :min-child-weight 1.0,
                           :rabit-timeout -1,
                           :colsample-bylevel 1.0,
                           :nthread 1,
                           :max-bin 16,
                           :seed 0,
                           :label-col "label",
                           :tree-method "auto",
                           :normalize-type "tree",
                           :checkpoint-interval -1,
                           :sample-type "uniform",
                           :probability-col "probability",
                           :gamma 0.0,
                           :alpha 0.0,
                           :skip-drop 0.0,
                           :rabit-ring-reduce-threshold 32768,
                           :train-test-ratio 1.0,
                           :grow-policy "depthwise",
                           :lambda-bias 0.0,
                           :use-external-memory false,
                           :rate-drop 0.0,
                           :tree-limit 0,
                           :objective "reg:squarederror",
                           :missing 0.0,
                           :max-depth 6,
                           :custom-eval nil,
                           :sketch-eps 0.03,
                           :custom-obj nil,
                           :max-delta-step 0.0,
                           :colsample-bytree 1.0,
                           :prediction-col "prediction",
                           :timeout-request-workers 1800000,
                           :features-col "features",
                           :eta 0.3,
                           :base-score 0.5}
                max-bin   (coalesce (:max-bin params)
                                    (:max-bins params)
                                    (:max-bin defaults))
                props     (-> defaults
                              (merge params)
                              (assoc :max-bins max-bin))]
            (interop/instantiate XGBoostClassifier props)))

      (defn xgboost-regressor [params]
        (let [defaults  {:num-round 1,
                         :dmlc-worker-connect-retry 5,
                         :subsample 1.0,
                         :num-early-stopping-rounds 0,
                         :cache-training-set false,
                         :allow-non-zero-for-missing false,
                         :checkpoint-path "",
                         :verbosity 1,
                         :scale-pos-weight 1.0,
                         :lambda 1.0,
                         :silent 0,
                         :num-workers 1,
                         :min-child-weight 1.0,
                         :rabit-timeout -1,
                         :colsample-bylevel 1.0,
                         :nthread 1,
                         :max-bin 16,
                         :seed 0,
                         :label-col "label",
                         :tree-method "auto",
                         :normalize-type "tree",
                         :checkpoint-interval -1,
                         :sample-type "uniform",
                         :gamma 0.0,
                         :alpha 0.0,
                         :skip-drop 0.0,
                         :rabit-ring-reduce-threshold 32768,
                         :train-test-ratio 1.0,
                         :grow-policy "depthwise",
                         :lambda-bias 0.0,
                         :use-external-memory false,
                         :rate-drop 0.0,
                         :tree-limit 0,
                         :objective "reg:squarederror",
                         :missing 0.0,
                         :max-depth 6,
                         :custom-eval nil,
                         :sketch-eps 0.03,
                         :custom-obj nil,
                         :max-delta-step 0.0,
                         :colsample-bytree 1.0,
                         :prediction-col "prediction",
                         :timeout-request-workers 1800000,
                         :features-col "features",
                         :eta 0.3,
                         :base-score 0.5}
              max-bin   (coalesce (:max-bin params)
                                  (:max-bins params)
                                  (:max-bin defaults))
              props     (-> defaults
                            (merge params)
                            (assoc :max-bins max-bin))]
          (interop/instantiate XGBoostRegressor props)))

     :succeeded)

   :failed))

(include-xgboost-fns false)
