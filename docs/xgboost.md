## Optional XGBoost Support

Geni wraps [XGBoost4J-Spark](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html)'s estimators as `ml/xgboost-classifier` and `ml/xgboost-regressor`, and saves a trained model's native booster with `ml/write-native-model!`. XGBoost4J isn't one of Geni's dependencies: when it's on the classpath as `zero-one.geni.ml` loads, those functions use it, and otherwise they throw an error that says what to add.

Geni's XGBoost tests, in `test-xgb/`, use XGBoost4J 1.2.0 on Spark 3.5 and Scala 2.12:

```edn
{:deps {ml.dmlc/xgboost4j-spark_2.12 {:mvn/version "1.2.0"}
        ml.dmlc/xgboost4j_2.12       {:mvn/version "1.2.0"}}}
```

That version predates Spark 3.5, has no build for Scala 2.13 or Spark 4, and fails to train on Apple Silicon. It needs `libgomp1` on Linux. The XGBoost tests don't run in CI, and Geni's defaults for the estimators' params come from XGBoost4J 1.2.

An example of using XGBoost for classification:

<!-- :test-doc-blocks/skip -->
```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def xgb-model
  (ml/fit training (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-model)
    (g/select :label :probability)
    (g/limit 5)
    g/show)
```
