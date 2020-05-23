# Geni Examples

The examples assume the following required namespaces:

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])
```

and a spark session, which can be defined as:

```clojure
(defonce spark (g/create-spark-session {}))
```

Example datasets can be found in the `test/resources` directory.

## Dataframe API

The following examples are taken from [Apache Spark's example page](https://spark.apache.org/examples.html) and [Databricks' examples](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html).

### Text Search

```clojure
(-> melbourne-df
    (g/filter (g/like "Suburb" "%South%"))
    (g/select "Suburb")
    g/distinct
    g/show)

; +----------------+
; |Suburb          |
; +----------------+
; |South Melbourne |
; |South Kingsville|
; |Clayton South   |
; |Blackburn South |
; |Vermont South   |
; |Caulfield South |
; |Croydon South   |
; |Springvale South|
; |Melton South    |
; |Oakleigh South  |
; |Wantirna South  |
; |Southbank       |
; |South Morang    |
; |Frankston South |
; |South Yarra     |
; +----------------+
```

### Printing Schema

``` clojure
(-> melbourne-df
    g/print-schema)

; root
;  |-- Suburb: string (nullable = true)
;  |-- Address: string (nullable = true)
;  |-- Rooms: long (nullable = true)
;  |-- Type: string (nullable = true)
;  |-- Price: double (nullable = true)
;  |-- Method: string (nullable = true)
;  |-- SellerG: string (nullable = true)
;  |-- Date: string (nullable = true)
;  |-- Distance: double (nullable = true)
;  |-- Postcode: double (nullable = true)
;  |-- Bedroom2: double (nullable = true)
;  |-- Bathroom: double (nullable = true)
;  |-- Car: double (nullable = true)
;  |-- Landsize: double (nullable = true)
;  |-- BuildingArea: double (nullable = true)
;  |-- YearBuilt: double (nullable = true)
;  |-- CouncilArea: string (nullable = true)
;  |-- Lattitude: double (nullable = true)
;  |-- Longtitude: double (nullable = true)
;  |-- Regionname: string (nullable = true)
;  |-- Propertycount: double (nullable = true)
```

### Descriptive Statistics

```clojure
(-> melbourne-df
    (g/describe "Price")
    g/show)

; +-------+-----------------+
; |summary|Price            |
; +-------+-----------------+
; |count  |13580            |
; |mean   |1075684.079455081|
; |stddev |639310.7242960163|
; |min    |85000.0          |
; |max    |9000000.0        |
; +-------+-----------------+
```

### Null Rates

```clojure
(let [null-rate-cols (map g/null-rate (g/column-names melbourne-df))]
  (-> melbourne-df
      (g/agg null-rate-cols)
      g/show-vertical))

; -RECORD 0----------------------------------------
;  null_rate(Suburb)        | 0.0
;  null_rate(Address)       | 0.0
;  null_rate(Rooms)         | 0.0
;  null_rate(Type)          | 0.0
;  null_rate(Price)         | 0.0
;  null_rate(Method)        | 0.0
;  null_rate(SellerG)       | 0.0
;  null_rate(Date)          | 0.0
;  null_rate(Distance)      | 0.0
;  null_rate(Postcode)      | 0.0
;  null_rate(Bedroom2)      | 0.0
;  null_rate(Bathroom)      | 0.0
;  null_rate(Car)           | 0.004565537555228277
;  null_rate(Landsize)      | 0.0
;  null_rate(BuildingArea)  | 0.47496318114874814
;  null_rate(YearBuilt)     | 0.3958026509572901
;  null_rate(CouncilArea)   | 0.1008100147275405
;  null_rate(Lattitude)     | 0.0
;  null_rate(Longtitude)    | 0.0
;  null_rate(Regionname)
;   | 0.0
;  null_rate(Propertycount) | 0.0
```

## MLlib

The following examples are taken from [Apache Spark's MLlib guide](https://spark.apache.org/docs/latest/ml-guide.html).

### Correlation

```clojure
(def corr-df
  (g/table->dataset
    spark
    [[[1.0 0.0 -2.0 0.0]]
     [[4.0 5.0 0.0  3.0]]
     [[6.0 7.0 0.0  8.0]]
     [[9.0 0.0 1.0  0.0]]]
    [:features]))

(let [corr-kw (keyword "pearson(features)")]
  (corr-kw (g/first (ml/corr corr-df "features"))))
; => ((1.0                  0.055641488407465814 0.9442673704375603  0.1311482458941057)
;     (0.055641488407465814 1.0                  0.22329687826943603 0.9428090415820635)
;     (0.9442673704375603   0.22329687826943603  1.0                 0.19298245614035084)
;     (0.1311482458941057   0.9428090415820635   0.19298245614035084 1.0))
```

### Hypothesis Testing

```clojure
(def hypothesis-df
  (g/table->dataset
     spark
     [[0.0 [0.5 10.0]]
      [0.0 [1.5 20.0]]
      [1.0 [1.5 30.0]]
      [0.0 [3.5 30.0]]
      [0.0 [3.5 40.0]]
      [1.0 [3.5 40.0]]]
     [:label :features]))

(g/first (ml/chi-square-test hypothesis-df "features" "label"))
; => {:pValues (0.6872892787909721 0.6822703303362126),
;     :degreesOfFreedom (2 3),
;     :statistics (0.75 1.5))
```

### Tokeniser, Hashing TF and IDF

```clojure
(def sentence-data
  (g/table->dataset
    spark
    [[0.0 "Hi I heard about Spark"]
     [0.0 "I wish Java could use case classes"]
     [1.0 "Logistic regression models are neat"]]
    [:label :sentence]))

(def pipeline
  (ml/pipeline
    (ml/tokenizer {:input-col "sentence"
                    :output-col "words"})
    (ml/hashing-tf {:num-features 20
                    :input-col "words"
                    :output-col "raw-features"})
    (ml/idf {:input-col "raw-features"
              :output-col "features"})))

(def pipeline-model
  (ml/fit sentence-data pipeline))

(-> sentence-data
    (ml/transform pipeline-model)
    (g/collect-col "features"))

; => ((0.6931471805599453
;      0.6931471805599453
;      0.28768207245178085
;      1.3862943611198906)
;     (0.6931471805599453
;      0.6931471805599453
;      0.8630462173553426
;      0.28768207245178085
;      0.28768207245178085)
;     (0.6931471805599453
;      0.6931471805599453
;      0.28768207245178085
;      0.28768207245178085
;      0.6931471805599453))
```

### PCA

```clojure
(def dataframe
  (g/table->dataset
    spark
    [[[0.0 1.0 0.0 7.0 0.0]]
     [[2.0 0.0 3.0 4.0 5.0]]
     [[4.0 0.0 0.0 6.0 7.0]]]
    [:features]))

(def pca
  (ml/fit dataframe (ml/pca {:input-col "features"
                             :output-col "pca-features"
                             :k 3})))

(-> dataframe
    (ml/transform pca)
    (g/collect-col "pca-features"))

;; => ((1.6485728230883807 -4.013282700516296 -5.524543751369388)
;;     (-4.645104331781534 -1.1167972663619026 -5.524543751369387)
;;     (-6.428880535676489 -5.337951427775355 -5.524543751369389))
```

### Standard Scaler

```clojure
(def scaler
  (ml/standard-scaler {:input-col "features"
                       :output-col "scaled-features"
                       :with-std true
                       :with-mean false}))

(def scaler-model (ml/fit libsvm-df scaler))

(-> libsvm-df
    (ml/transform scaler-model)
    (g/limit 1)
    (g/collect-col "scaled-features"))

;; => ((0.5468234998110156
;;      1.5923262059067456
;;      2.435399721310935
;;      1.7081091742536456
;;      0.7334796787587756
;;      0.43457146586677264
;;      2.0985334204247876
;;      2.2563158921609334
;;      2.236765962167892
;;      2.226905085275203
;;      2.2554541846497917
;;      ...
```

### Vector Assembler

```clojure
(def dataset
  (g/table->dataset
    spark
    [[0 18 1.0 [0.0 10.0 0.5] 1.0]]
    [:id :hour :mobile :user-features :clicked]))

(def assembler
  (ml/vector-assembler {:input-cols ["hour" "mobile" "user-features"]
                        :output-col "features"}))

(-> dataset
    (ml/transform assembler)
    (g/select "features" "clicked")
    g/show)

; +-----------------------+-------+
; |features               |clicked|
; +-----------------------+-------+
; |[18.0,1.0,0.0,10.0,0.5]|1.0    |
; +-----------------------+-------+
```

### Logistic Regression

```clojure
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def lr (ml/logistic-regression {:max-iter 10
                                 :reg-param 0.3
                                 :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select "label" "probability")
    (g/limit 5)
    g/show)

; +-----+----------------------------------------+
; |label|probability                             |
; +-----+----------------------------------------+
; |0.0  |[0.6764827243160599,0.32351727568394006]|
; |1.0  |[0.22640965216205314,0.7735903478379468]|
; |1.0  |[0.2210316383828499,0.7789683616171501] |
; |1.0  |[0.2526490765347194,0.7473509234652805] |
; |1.0  |[0.22494007343582254,0.7750599265641774]|
; +-----+----------------------------------------+
```

### Gradient Boosted Tree Classifier

```clojure
(def data (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def split-data (g/random-split data [0.7 0.3]))
(def train-data (first split-data))
(def test-data (second split-data))

(def label-indexer
  (ml/fit data (ml/string-indexer {:input-col "label" :output-col "indexed-label"})))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col "features"
                                   :output-col "indexed-features"
                                   :max-categories 4})))

(def pipeline
  (ml/pipeline
    label-indexer
    feature-indexer
    (ml/gbt-classifier {:label-col "indexed-label"
                        :features-col "indexed-features"
                        :max-iter 10
                        :feature-subset-strategy "auto"})
    (ml/index-to-string {:input-col "prediction"
                         :output-col "predicted-label"
                         :labels (.labels label-indexer)})))

(def model (ml/fit train-data pipeline))

(def predictions (ml/transform test-data model))

(def evaluator
  (ml/multiclass-classification-evaluator {:label-col "indexed-label"
                                           :prediction-col "prediction"
                                           :metric-name "accuracy"}))

(-> predictions
    (g/select "predicted-label" "label")
    (g/order-by (g/rand)))
(println "Test error:" (- 1 (ml/evaluate predictions evaluator)))

; +---------------+-----+
; |predicted-label|label|
; +---------------+-----+
; |1.0            |1.0  |
; |1.0            |1.0  |
; |1.0            |1.0  |
; |1.0            |1.0  |
; |1.0            |1.0  |
; +---------------+-----+
;
; Test error: 0.08823529411764708
```

#### Linear Regression

```clojure
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def lr (ml/linear-regression {:max-iter 10
                               :reg-param 0.8
                               :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

; +-----+----------+
; |label|prediction|
; +-----+----------+
; |0.0  |0.57      |
; |1.0  |0.57      |
; |1.0  |0.57      |
; |1.0  |0.57      |
; |1.0  |0.57      |
; +-----+----------+
```

### Random Forest Regression

```clojure
(def data (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col "features"
                                   :output-col "indexed-features"
                                   :max-categories 4})))

(def split-data (g/random-split data [0.7 0.3]))
(def train-data (first split-data))
(def test-data (second split-data))

(def pipeline
  (ml/pipeline
    feature-indexer
    (ml/random-forest-regressor {:label-col "label"
                                 :features-col "indexed-features"})))

(def model (ml/fit train-data pipeline))
(def predictions (ml/transform test-data model))
(def evaluator
  (ml/regression-evaluator {:label-col "label"
                            :prediction-col "prediction"
                            :metric-name "rmse"}))

(-> predictions
    (g/select "prediction" "label")
    (g/show {:num-rows 5}))
(println "RMSE:" (ml/evaluate predictions evaluator))

; +----------+-----+
; |prediction|label|
; +----------+-----+
; |0.15      |0.0  |
; |0.05      |0.0  |
; |0.05      |0.0  |
; |0.0       |0.0  |
; |0.15      |0.0  |
; +----------+-----+
;
; RMSE: 0.1436762233038478
```

#### Survival Regression

```clojure
(def train
  (g/table->dataset
    spark
    [[1.218 1.0 [1.560 -0.605]]
     [2.949 0.0 [0.346  2.158]]
     [3.627 0.0 [1.380  0.231]]
     [0.273 1.0 [0.520  1.151]]
     [4.199 0.0 [0.795 -0.226]]]
    [:label :censor :features]))

(def quantile-probabilities [0.3 0.6])

(def aft
  (ml/aft-survival-regression
    {:quantile-probabilities quantile-probabilities
     :quantiles-col "quantiles"}))

(def aft-model (ml/fit train aft))

(-> train (ml/transform model) g/show)

; +-----+------+--------------+------------------+---------------------------------------+
; |label|censor|features      |prediction        |quantiles                              |
; +-----+------+--------------+------------------+---------------------------------------+
; |1.218|1.0   |[1.56,-0.605] |5.718979487634987 |[1.1603238947151624,4.9954560102747525]|
; |2.949|0.0   |[0.346,2.158] |18.076521181495465|[3.6675458454717664,15.789611866277742]|
; |3.627|0.0   |[1.38,0.231]  |7.381861804239099 |[1.4977061305190835,6.447962612338963] |
; |0.273|1.0   |[0.52,1.151]  |13.57761250142532 |[2.7547621481506925,11.859872224069731]|
; |4.199|0.0   |[0.795,-0.226]|9.013097744073866 |[1.8286676321297761,7.872826505878401] |
; +-----+------+--------------+------------------+---------------------------------------+
```
