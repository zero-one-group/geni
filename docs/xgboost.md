## Optional XGBoost Support

Geni will automatically detect whether the following optional dependencies are present:

```clojure
[ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
[ml.dmlc/xgboost4j_2.12 "1.0.0"]
```

If so, the vars `ml/xgboost-classifier` and `ml/xgboost-regressor` will be bound to the appropriate functions. Otherwise, the vars will remain unbound.

An example of using XGBoost for classification:

```
(def xgb-model
  (ml/fit
    training
    (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> testing
    (ml/transform xgb-model)
    (g/select :label :probability)
    (g/limit 5)
    g/show)
```
