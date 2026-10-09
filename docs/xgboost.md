## Optional XGBoost Support

Geni wraps [XGBoost4J-Spark](https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html)'s estimators as `ml/xgboost-classifier`, `ml/xgboost-regressor` and `ml/xgboost-ranker`, and saves a trained model's native booster with `ml/write-native-model!`. XGBoost4J-Spark isn't one of Geni's dependencies: the estimators find it on the classpath when they're called, and otherwise throw an error that says what to add.

Geni's XGBoost tests use XGBoost4J-Spark 3.4.0, whose jar includes XGBoost4J itself. The artifact is for your Scala version, `_2.12` for Spark 3.5 on Scala 2.12, and `_2.13` otherwise:

```edn
{:deps {ml.dmlc/xgboost4j-spark_2.12 {:mvn/version "3.4.0"}}}
```

XGBoost4J-Spark 3 is built against Spark 3.5, and also works on Spark 4.0.1 and later, apart from saving as a Spark ML stage (see Saving), but not on 4.0.0. Its native library needs OpenMP: `libgomp1` on Linux, and `brew install libomp` on macOS. It needs classic Spark, as Spark ML does.

The params are XGBoost's own, in kebab case, such as `:num-round`, `:max-depth`, `:eta` and `:objective`, and so are the defaults, such as 100 rounds. `:max-bin` is another name for `:max-bins`, the name of its setter.

XGBoost4J-Spark 3.4.0, the latest on Maven Central, predicts wrongly on sparse feature vectors, such as `g/read-libsvm!`'s and often `ml/vector-assembler`'s. XGBoost fixed that in 3.4.1, which isn't on Maven Central. So turn sparse vectors into arrays with `ml/vector-to-array`, which XGBoost4J-Spark 3 takes as features too. Fitting on sparse vectors also needs `:missing`, which XGBoost won't assume for them.

The examples use the LIBSVM data in Geni's repo, with its features as arrays, and a few rounds of shallow trees. They round the regressor's and ranker's predictions to three places, since the last digits can differ between machines:

<!-- #:test-doc-blocks{:meta :xgb :apply :all-next} -->
```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(def training
  (-> (g/read-libsvm! "test/resources/sample_libsvm_data.txt")
      (g/with-column :features (ml/vector-to-array :features))))
```

## Classification

```clojure
(def classifier-model
  (ml/fit training (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform classifier-model)
    (g/select :label :prediction)
    (g/limit 5)
    g/collect)
;; => ({:label 0.0, :prediction 0.0}
;;     {:label 1.0, :prediction 1.0}
;;     {:label 1.0, :prediction 1.0}
;;     {:label 1.0, :prediction 1.0}
;;     {:label 1.0, :prediction 1.0})
```

## Regression

```clojure
(def regressor-model
  (ml/fit training (ml/xgboost-regressor {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform regressor-model)
    (g/select :label {:prediction (g/expr "round(prediction, 3)")})
    (g/limit 5)
    g/collect)
;; => ({:label 0.0, :prediction 0.285}
;;     {:label 1.0, :prediction 0.786}
;;     {:label 1.0, :prediction 0.786}
;;     {:label 1.0, :prediction 0.786}
;;     {:label 1.0, :prediction 0.786})
```

## Ranking

A ranker learns the order of rows within each group, such as the results of one query, which `:group-col` names:

```clojure
(def queries
  (g/with-column training :query (g/int (g/mod (g/monotonically-increasing-id) 4))))

(def ranker-model
  (ml/fit queries (ml/xgboost-ranker {:group-col "query" :max-depth 2 :num-round 2})))

(-> queries
    (ml/transform ranker-model)
    (g/select :query :label {:prediction (g/expr "round(prediction, 3)")})
    (g/limit 5)
    g/collect)
;; => ({:query 0, :label 0.0, :prediction -0.446}
;;     {:query 1, :label 1.0, :prediction 0.453}
;;     {:query 2, :label 1.0, :prediction 0.453}
;;     {:query 3, :label 1.0, :prediction 0.453}
;;     {:query 0, :label 1.0, :prediction 0.453})
```

## Saving

`ml/write-native-model!` saves XGBoost's booster, which XGBoost's other bindings, such as Python's, can load:

```clojure
(ml/write-native-model! classifier-model "target/xgb-classifier.json")
```

On Spark 3.5, a trained model also saves as a Spark ML stage with `ml/write-stage!`, and loads again with `ml/read-stage!` and its class, here `XGBoostClassificationModel`. On Spark 4, `ml/write-stage!` fails with a `NoSuchMethodError` from json4s, since XGBoost4J-Spark 3.4.0 is built against Spark 3.5's json4s, so a pipeline with an XGBoost model in it doesn't save either:

<!-- #:test-doc-blocks{:meta {:xgb true :spark-3 true}} -->
```clojure
(ml/write-stage! classifier-model "target/xgb-classifier" {:mode "overwrite"})

(def loaded-model
  (ml/read-stage! (class classifier-model) "target/xgb-classifier"))

(= (-> training (ml/transform classifier-model) (g/collect-col :prediction))
   (-> training (ml/transform loaded-model) (g/collect-col :prediction)))
;; => true
```
