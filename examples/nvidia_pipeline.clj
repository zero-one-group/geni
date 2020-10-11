(ns examples.nvidia-pipeline
  (:require
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml])
  (:import
   (org.apache.spark.ml.tuning CrossValidatorModel)))

;; Load Data
(def dataframe
  (-> (g/read-parquet! "test/resources/housing.parquet")
      (g/with-column :rooms_per_house (g// :total_rooms :households))
      (g/with-column :population_per_house (g// :population :households))
      (g/with-column :bedrooms_per_house (g// :total_bedrooms :households))
      (g/drop :total_rooms :households :population :total_bedrooms)
      (g/with-column :median_income (g/double :median_income))
      (g/with-column :median_house_value (g/double :median_house_value))
      (g/with-column :housing_median_age (g/double :housing_median_age))
      g/cache))

;; Summary Statistics
(-> dataframe
    (g/describe :median_income
                :median_house_value
                :bedrooms_per_house
                :population_per_house)
    g/show)

;;=>
;; +-------+------------------+------------------+------------------+--------------------+
;; |summary|median_income     |median_house_value|bedrooms_per_house|population_per_house|
;; +-------+------------------+------------------+------------------+--------------------+
;; |count  |5000              |5000              |4947              |5000                |
;; |mean   |3.4760238599999966|177071.724        |1.119226894634528 |3.0392645333005506  |
;; |stddev |1.8424610040929013|107669.60822163108|0.741916662913725 |8.484712111958718   |
;; |min    |0.4999            |100000.0          |0.5               |1.0661764705882353  |
;; |max    |9.8708            |99800.0           |34.06666666666667 |599.7142857142857   |
;; +-------+------------------+------------------+------------------+--------------------+

(-> dataframe
    (g/select (g/corr :median_house_value :median_income))
    g/show)

;;=>
;; +---------------------------------------+
;; |corr(median_house_value, median_income)|
;; +---------------------------------------+
;; |0.6747425007394737                     |
;; +---------------------------------------+

(def dataframe-splits (g/random-split dataframe [0.8 0.2] 1234))
(def training-data (first dataframe-splits))
(def test-data (second dataframe-splits))

;; Feature Extraction and Pipelining
(def assembler
  (ml/vector-assembler {:input-cols [:housing_median_age
                                     :median_income
                                     :bedrooms_per_house
                                     :population_per_house]
                        :output-col :raw-features
                        :handle-invalid "skip"}))

(def scaler
  (ml/standard-scaler {:input-col :raw-features
                       :output-col :features
                       :with-mean true
                       :with-std true}))

(def random-forest
  (ml/random-forest-regressor {:label-col :median_house_value
                               :features-col :features}))

(def pipeline
  (ml/pipeline assembler scaler random-forest))

;; Model Training
(def param-grid
  (ml/param-grid
   {random-forest {:max-bins (map int [100 200])
                   :max-depth (map int [2 7 10])
                   :num-trees (map int [5 20])}}))

(def evaluator
  (ml/regression-evaluator {:label-col :median_house_value
                            :prediction-col :prediction
                            :metric-name "rmse"}))

(def cross-validator
  (ml/cross-validator {:estimator pipeline
                       :evaluator evaluator
                       :estimator-param-maps param-grid
                       :num-folds 3}))

;; Note: this will take time to run
(def pipeline-model (ml/fit training-data cross-validator))

(def feature-importances
  (-> pipeline-model
      ml/best-model
      ml/stages
      last
      ml/feature-importances))

(println (zipmap (ml/input-cols assembler) feature-importances))
;;=>
;; {"housing_median_age" 0.09494223585028874,
;;  "median_income" 0.6703451256623851,
;;  "bedrooms_per_house" 0.07028730004514734,
;;  "population_per_house" 0.1644253384421788)

;; Predictions and Model Evaluations
(def predictions
  (-> test-data
      (ml/transform pipeline-model)
      (g/select :prediction :median_house_value)
      (g/with-column :error (g/- :prediction :median_house_value))))

(-> predictions (g/select :prediction :median_house_value :error) g/show)

;;=>
;; +------------------+------------------+------------------+
;; |prediction        |median_house_value|error             |
;; +------------------+------------------+------------------+
;; |85944.55351293649 |56100.0           |29844.553512936487|
;; |103897.96820375702|45000.0           |58897.96820375702 |
;; |93095.44004842117 |25000.0           |68095.44004842117 |
;; |265395.44891077984|146300.0          |119095.44891077984|
;; |88971.00633859796 |88400.0           |571.0063385979593 |
;; |146160.32523296136|142500.0          |3660.3252329613606|
;; |95449.72190951569 |43300.0           |52149.72190951569 |
;; |105709.93194635629|79200.0           |26509.931946356286|
;; |92407.146575551   |61200.0           |31207.146575551   |
;; |104356.10455364344|62200.0           |42156.10455364344 |
;; |97861.58630282713 |60400.0           |37461.58630282713 |
;; |154068.91014066993|70800.0           |83268.91014066993 |
;; |180527.08814247025|68600.0           |111927.08814247025|
;; |231390.56724255547|110100.0          |121290.56724255547|
;; |148634.55789243293|87500.0           |61134.55789243293 |
;; |276028.89366370626|159900.0          |116128.89366370626|
;; |87272.81905234624 |112500.0          |-25227.18094765376|
;; |144523.3925025806 |125000.0          |19523.392502580595|
;; |93695.49142680826 |50000.0           |43695.491426808265|
;; |270681.11345725175|115700.0          |154981.11345725175|
;; +------------------+------------------+------------------+
;; only showing top 20 rows

(-> predictions (g/describe :prediction :median_house_value :error) g/show)
;;=>
;; +-------+-----------------+------------------+------------------+
;; |summary|prediction       |median_house_value|error             |
;; +-------+-----------------+------------------+------------------+
;; |count  |979              |979               |979               |
;; |mean   |175724.3365192136|171209.6251276813 |4514.711391532304 |
;; |stddev |78324.3027009389 |101721.69616397416|64162.229024491324|
;; |min    |82657.8386042213 |25000.0           |-363229.0007937625|
;; |max    |478487.5783698894|500001.0          |322249.77625      |
;; +-------+-----------------+------------------+------------------+

(def mae
  (ml/evaluate
   predictions
   (ml/regression-evaluator {:label-col :median_house_value :metric-name "mae"})))

(def rmse
  (ml/evaluate
   predictions
   (ml/regression-evaluator {:label-col :median_house_value :metric-name "rmse"})))

(println (format "MAE: %,.2f, RMSE: %,.2f" mae rmse))

;; MAE: 48,558.07, RMSE: 64,288.17

;; Saving and Loading
(ml/write-stage! pipeline-model "target/nvidia_pipeline_model" {:mode "overwrite"})

;; TODO: NoSuchElementException
(def same-model
  (ml/read-stage! CrossValidatorModel "target/nvidia_pipeline_model"))
