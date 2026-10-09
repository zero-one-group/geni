(ns zero-one.geni.ml.regression
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]]))

(interop/def-stages org.apache.spark.ml.regression
  [aft-survival-regression AFTSurvivalRegression]
  [decision-tree-regressor DecisionTreeRegressor]
  [fm-regressor FMRegressor]
  [gbt-regressor GBTRegressor]
  [generalized-linear-regression GeneralizedLinearRegression]
  [isotonic-regression IsotonicRegression]
  [linear-regression LinearRegression]
  [random-forest-regressor RandomForestRegressor])

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.regression
 [(-> docs/spark-docs :classes :ml :regression)])

;; Aliases
(import-fn generalized-linear-regression generalised-linear-regression)
(import-fn generalized-linear-regression glm)
