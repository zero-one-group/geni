(ns examples.xgboost4j-tutorial
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]])
  (:import
    (ml.dmlc.xgboost4j.scala.spark XGBoostClassificationModel)))

(def raw-input
  (-> (g/read-csv! spark "resources/iris.data" {:header false})
      (g/select {:sepal-length (g/double :_c0)
                 :sepal-width  (g/double :_c1)
                 :petal-length (g/double :_c2)
                 :petal-width  (g/double :_c3)
                 :class        :_c4})))

(def string-indexer
  (ml/fit
    raw-input
    (ml/string-indexer {:input-col  :class
                        :output-col :class-index})))

(def label-transformed
  (-> raw-input
      (ml/transform string-indexer)
      (g/drop :class)))

(def vector-assembler
  (ml/vector-assembler {:input-cols [:sepal-length
                                     :sepal-width
                                     :petal-length
                                     :petal-width]
                        :output-col :features}))

(def xgb-input
  (-> label-transformed
      (ml/transform vector-assembler)
      (g/select :features :class-index)))

(def xgb-classifier
  (ml/xgboost-classifier {:eta          0.1
                          :missing      -999
                          :num-round    100
                          :num-workers  2
                          :features-col :features
                          :label-col    :class-index}))

(def xgb-classifier-model
  (ml/fit xgb-input xgb-classifier))

(def predictions
  (-> xgb-input
      (ml/transform xgb-classifier-model)
      (g/select
        :class-index
        (g/element-at (ml/vector->array "probability") 1))))

(ml/write-stage! xgb-classifier-model "resources/xgb_classification_model" {:mode "overwrite"})

(def xgb-classifier-model-2
  (ml/read-stage! XGBoostClassificationModel "resources/xgb_classification_model"))

(ml/write-native-model! xgb-classifier-model "resources/native_xgb_classification_model")
