# Changelog

## 0.1.0 (unreleased)

Breaking changes:

- Geni now targets Spark 3.5 on JDK 17 or 21. Spark 3.4 and older, and JDK 8 and 11, are no longer supported.
- `g/nunique` keys its counts by column name, e.g. `{:SellerG 6 :Suburb 1}`, instead of Spark's `count(DISTINCT SellerG)`.
- The Leiningen template is retired. Geni itself now builds with the Clojure CLI, but you can still depend on it from Leiningen.

Fixes:

- Without XGBoost on the classpath, `ml/xgboost-classifier`, `ml/xgboost-regressor` and `ml/write-native-model!` now throw a clear error instead of being unbound.
- `collect-to-arrow` works on JDK 21 with Spark 3.5, as long as Arrow 13 or newer is on the classpath. Spark 3.5 ships Arrow 12, which can't allocate buffers on JDK 21.
