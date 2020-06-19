(ns examples.xgboost
  (:require
    [zero-one.geni.core :as g]))

;(defmacro with-dynamic-import [imports & body]
  ;(if (try
        ;(eval imports)
        ;(catch ClassNotFoundException _ nil))
    ;`(do ~@body :succeeded)
    ;:failed))

;(with-dynamic-import
  ;(import '(ml.dmlc.xgboost4j.scala.spark XGBoostClassifier XGBoostRegressor))
  ;(def x (XGBoostRegressor.)))
;x
