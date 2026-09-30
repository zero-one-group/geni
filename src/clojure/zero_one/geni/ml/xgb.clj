(ns zero-one.geni.ml.xgb
  "XGBoost4J-Spark's estimators, when XGBoost4J-Spark 3 is on the classpath.
  See the XGBoost guide."
  (:require
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [with-dynamic-import]]))

(declare xgboost-classifier
         xgboost-regressor
         xgboost-ranker
         write-native-model!)

(defn- xgboost-params
  "The params as XGBoost4J-Spark's setters take them. `:max-bin`, the name that
  XGBoost and `ml/params` use, stands for `:max-bins`, the setter's."
  [params]
  (if (contains? params :max-bin)
    (-> params (dissoc :max-bin) (assoc :max-bins (:max-bin params)))
    params))

(with-dynamic-import
  [[ml.dmlc.xgboost4j.scala.spark XGBoostClassifier XGBoostRegressor]]

  (defn xgboost-classifier
    "A gradient-boosted tree classifier, XGBoost4J-Spark's `XGBoostClassifier`.

    The params are XGBoost's own, in kebab case, such as `:num-round`,
    `:max-depth`, `:eta` and `:objective`, and so are the defaults: 100 rounds,
    for one. `:max-bin` is another name for `:max-bins`.

    XGBoost4J-Spark 3.4.0 predicts wrongly on sparse feature vectors, such as
    LIBSVM data's, so turn them into arrays with `ml/vector-to-array` first.

    XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
    [params]
    (interop/instantiate XGBoostClassifier (xgboost-params params)))

  (defn xgboost-regressor
    "A gradient-boosted tree regressor, XGBoost4J-Spark's `XGBoostRegressor`.
    The params are as for `xgboost-classifier`.

    XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
    [params]
    (interop/instantiate XGBoostRegressor (xgboost-params params)))

  (defn write-native-model!
    "Saves a trained XGBoost model's native `Booster` to a file at `path`. It
    works on Spark 4, where `ml/write-stage!` doesn't for XGBoost models."
    [model path]
    (-> model .nativeBooster (.saveModel ^String path))))

(with-dynamic-import
  [[ml.dmlc.xgboost4j.scala.spark XGBoostRanker]]

  (defn xgboost-ranker
    "A learning-to-rank estimator, XGBoost4J-Spark's `XGBoostRanker`, which
    needs `:group-col`, the column that groups the rows into queries. The
    other params are as for `xgboost-classifier`.

    XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
    [params]
    (interop/instantiate XGBoostRanker (xgboost-params params))))

;; Without XGBoost4J-Spark on the classpath, the vars above stay unbound. Give
;; them a clear error and a docstring instead.
(defn- xgboost-missing [& _]
  (throw (ex-info (str "XGBoost4J-Spark 3 isn't on the classpath. Add "
                       "ml.dmlc/xgboost4j-spark_2.12 or _2.13, for your Scala, to your "
                       "dependencies to use it.")
                  {})))

(doseq [[v arglists] [[#'xgboost-classifier '([params])]
                      [#'xgboost-regressor '([params])]
                      [#'xgboost-ranker '([params])]
                      [#'write-native-model! '([model path])]]
        :when (not (bound? v))]
  (alter-var-root v (constantly xgboost-missing))
  (alter-meta! v assoc
               :arglists arglists
               :doc "Needs the optional XGBoost4J-Spark 3 dependency (ml.dmlc/xgboost4j-spark_2.12 or _2.13)."))
