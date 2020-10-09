(ns geni.cookbook-11
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(load-file "docs/cookbook/cookbook-util.clj")

;; Part 11: Basic ML Pipelines

(download-data!
 "https://raw.githubusercontent.com/ageron/handson-ml/master/datasets/housing/housing.csv"
 "data/houses.csv")

(def houses
  (-> (g/read-csv! "data/houses.csv" {:kebab-columns true})
      (g/with-column :rooms-per-house (g// :total-rooms :households))
      (g/with-column :population-per-house (g// :population :households))
      (g/with-column :bedrooms-per-house (g// :total-bedrooms :households))
      (g/drop :total-rooms :households :population :total-bedrooms)
      (g/with-column :median-income (g/double :median-income))
      (g/with-column :median-house-value (g/double :median-house-value))
      (g/with-column :housing-median-age (g/double :housing-median-age))))

(g/print-schema houses)

;;=>
;; root
;;  |-- longitude: double (nullable = true)
;;  |-- latitude: double (nullable = true)
;;  |-- housing-median-age: double (nullable = true)
;;  |-- median-income: double (nullable = true)
;;  |-- median-house-value: double (nullable = true)
;;  |-- ocean-proximity: string (nullable = true)
;;  |-- rooms-per-house: double (nullable = true)
;;  |-- population-per-house: double (nullable = true)
;;  |-- bedrooms-per-house: double (nullable = true)

;; 11.1 Splitting into Train and Validation Sets

(def houses-splits (g/random-split houses [0.8 0.2] 1234))

(def training-data (first houses-splits))

(def test-data (second houses-splits))

(g/count training-data)
;;=> 16525

(g/count test-data)
;;=> 4115

;; 11.2 Model Pipelines
(def assembler
  (ml/vector-assembler {:input-cols [:housing-median-age
                                     :median-income
                                     :bedrooms-per-house
                                     :population-per-house]
                        :output-col :raw-features
                        :handle-invalid "skip"}))

(def scaler
  (ml/standard-scaler {:input-col :raw-features
                       :output-col :features
                       :with-mean true
                       :with-std true}))

(def random-forest
  (ml/random-forest-regressor {:label-col :median-house-value
                               :features-col :features}))

(def pipeline
  (ml/pipeline assembler scaler random-forest))

(def pipeline-model (ml/fit training-data pipeline))

(def predictions
  (-> test-data
      (ml/transform pipeline-model)
      (g/select :prediction :median-house-value)
      (g/with-column :error (g/- :prediction :median-house-value))))

(-> predictions (g/limit 5) g/show)

;;=>
;; +------------------+------------------+-----------------+
;; |prediction        |median-house-value|error            |
;; +------------------+------------------+-----------------+
;; |124351.25434440118|85800.0           |38551.25434440118|
;; |166946.9353283479 |111400.0          |55546.9353283479 |
;; |135896.6548560019 |70500.0           |65396.6548560019 |
;; |195527.8273169201 |128900.0          |66627.8273169201 |
;; |214557.50504524485|116100.0          |98457.50504524485|
;; +------------------+------------------+-----------------+

(let [evaluator (ml/regression-evaluator {:label-col :median-house-value
                                          :metric-name "mae"})]
  (println (format "MAE: %.2f" (ml/evaluate predictions evaluator))))

;;=> MAE: 54554.34

;; 11.3 Random Forest Feature Importances

(def feature-importances
  (->> pipeline-model
       ml/stages
       last
       ml/feature-importances
       (zipmap (ml/input-cols assembler))))

feature-importances

;;=>
;; {"housing-median-age" 0.060262475752573055,
;;  "median-income" 0.7847621702619059,
;;  "bedrooms-per-house" 0.010547166447551434,
;;  "population-per-house" 0.14442818753796965
