(ns zero-one.geni.ml.xgb
  "XGBoost4J-Spark's estimators, when XGBoost4J-Spark 3 is on the classpath.
  See the XGBoost guide."
  (:require
   [clojure.set :as set]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [class-named]]))

(defn- xgboost
  "XGBoost4J-Spark's estimator `class-name`, with `params` set. `:max-bin`,
  the name that XGBoost and `ml/params` use, stands for `:max-bins`, the
  setter's."
  [class-name params]
  (let [full-name (str "ml.dmlc.xgboost4j.scala.spark." class-name)]
    (interop/instantiate
     (or (class-named full-name)
         (throw (ex-info (str "XGBoost4J-Spark 3 isn't on the classpath. Add "
                              "ml.dmlc/xgboost4j-spark_2.12 or _2.13, for your Scala, to your "
                              "dependencies to use it.")
                         {:class full-name})))
     (set/rename-keys params {:max-bin :max-bins}))))

(defn xgboost-classifier
  "A gradient-boosted tree classifier, XGBoost4J-Spark's `XGBoostClassifier`.

  The params are XGBoost's own, in kebab case, such as `:num-round`,
  `:max-depth`, `:eta` and `:objective`, and so are the defaults: 100 rounds,
  for one. `:max-bin` is another name for `:max-bins`.

  XGBoost4J-Spark 3.4.0 predicts wrongly on sparse feature vectors, such as
  LIBSVM data's, so turn them into arrays with `ml/vector-to-array` first.

  XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
  [params]
  (xgboost "XGBoostClassifier" params))

(defn xgboost-regressor
  "A gradient-boosted tree regressor, XGBoost4J-Spark's `XGBoostRegressor`.
  The params are as for `xgboost-classifier`.

  XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
  [params]
  (xgboost "XGBoostRegressor" params))

(defn xgboost-ranker
  "A learning-to-rank estimator, XGBoost4J-Spark's `XGBoostRanker`, which
  needs `:group-col`, the column that groups the rows into queries. The
  other params are as for `xgboost-classifier`.

  XGBoost4J-Spark docs: https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html"
  [params]
  (xgboost "XGBoostRanker" params))

(defn write-native-model!
  "Saves a trained XGBoost model's native `Booster` to a file at `path`. It
  works on Spark 4, where `ml/write-stage!` doesn't for XGBoost models."
  [model path]
  (-> model .nativeBooster (.saveModel ^String path)))
