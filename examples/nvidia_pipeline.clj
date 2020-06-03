(ns examples.nvidia-pipeline
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

;; Load Data
(def dataframe
  (-> (g/read-parquet! spark "test/resources/housing.parquet")
      (g/with-column "rooms_per_house" (g// "total_rooms" "households"))
      (g/with-column "population_per_house" (g// "population" "households"))
      (g/with-column "bedrooms_per_house" (g// "total_bedrooms" "households"))
      (g/drop "total_rooms" "households" "population" "total_bedrooms")
      (g/with-column "median_income" (g/cast "median_income" "double"))
      (g/with-column "median_house_value" (g/cast "median_house_value" "double"))
      (g/with-column "housing_median_age" (g/cast "housing_median_age" "double"))
      g/cache))

;; Summary Statistics
(-> dataframe
    (g/describe "median_income"
                "median_house_value"
                "bedrooms_per_house"
                "population_per_house")
    g/show)
; +-------+------------------+------------------+------------------+--------------------+
; |summary|median_income     |median_house_value|bedrooms_per_house|population_per_house|
; +-------+------------------+------------------+------------------+--------------------+
; |count  |5000              |5000              |4947              |5000                |
; |mean   |3.4760238599999966|177071.724        |1.119226894634528 |3.0392645333005506  |
; |stddev |1.8424610040929013|107669.60822163108|0.741916662913725 |8.484712111958718   |
; |min    |0.4999            |100000.0          |0.5               |1.0661764705882353  |
; |max    |9.8708            |99800.0           |34.06666666666667 |599.7142857142857   |
; +-------+------------------+------------------+------------------+--------------------+

(-> dataframe
    (g/select (g/corr "median_house_value" "median_income"))
    g/show)
; +---------------------------------------+
; |corr(median_house_value, median_income)|
; +---------------------------------------+
; |0.6747425007394737                     |
; +---------------------------------------+

(def dataframe-splits (g/random-split dataframe [0.8 0.2] 1234))
(def training-data (first dataframe-splits))
(def test-data (second dataframe-splits))

;; Feature Extraction and Pipelining
(def assembler
  (ml/vector-assembler {:input-cols ["housing_median_age"
                                     "median_income"
                                     "bedrooms_per_house"
                                     "population_per_house"]
                        :output-col "raw-features"
                        :handle-invalid "skip"}))

(def scaler
  (ml/standard-scaler {:input-col "raw-features"
                       :output-col "features"
                       :with-mean true
                       :with-std true}))

(def random-forest
  (ml/random-forest-regressor {:label-col "median_house_value"
                               :features-col "features"}))

(def pipeline
  (ml/pipeline assembler scaler random-forest))

;; Model Training
(def param-grid
  (ml/param-grid
    {random-forest {:max-bins (map int [100 200])
                    :max-depth (map int [2 7 10])
                    :num-trees (map int [5 20])}}))

(def evaluator
  (ml/regression-evaluator {:label-col "median_house_value"
                            :prediction-col "prediction"
                            :metric-name "rmse"}))

(def cross-validator
  (ml/cross-validator {:estimator pipeline
                       :evaluator evaluator
                       :estimator-param-maps param-grid
                       :num-folds 3}))

(def pipeline-model (ml/fit training-data cross-validator))

(def feature-importances
  (-> pipeline-model
      .bestModel ; TODO
      ml/stages
      last
      ml/feature-importances))

; TODO
(println (zipmap (.getInputCols assembler) feature-importances))
; {"housing_median_age" 0.09494223585028874,
;  "median_income" 0.6703451256623851,
;  "bedrooms_per_house" 0.07028730004514734,
;  "population_per_house" 0.1644253384421788)

; Predictions and Model Evaluations

;val predictions = pipelineModel.transform(testData) predictions.select("prediction", "medhvalue").show(5)
;predictions = predictions.withColumn("error", col("prediction")-) col("medhvalue")
;predictions.select("prediction", "medhvalue", "error").show
;predictions.describe("prediction", "medhvalue", "error").show
;bal maevaluator = new RegressionEvaluator() .setLabelCol("medhvalue") .setMetricName("mae")
;val mae = maevaluator.evaluate(predictions)

;val evaluator = new RegressionEvaluator()
;.setLabelCol("medhvalue")
;.setMetricName("rmse")
;val rmse = evaluator.evaluate(predictions)

; Saving and Loading
;pipelineModel.write.overwrite().save(modeldir)
;val sameModel = CrossValidatorModel.load(“modeldir")
