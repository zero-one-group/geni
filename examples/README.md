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
    (g/limit 5)
    g/show)
; +----------------+
; |Suburb          |
; +----------------+
; |South Melbourne |
; |South Kingsville|
; |Clayton South   |
; |Blackburn South |
; |Vermont South   |
; +----------------+
```

### Printing Schema

``` clojure
(-> melbourne-df
    (g/select "Suburb" "Rooms" "Price")
    g/print-schema)
; root
;  |-- Suburb: string (nullable = true)
;  |-- Rooms: long (nullable = true)
;  |-- Price: double (nullable = true)
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
(letfn [(null-rate [col-name]
          (-> col-name
              g/null?
              (g/cast "double")
              g/mean
              (g/as col-name)))]
  (-> melbourne-df
      (g/agg (map null-rate ["Car" "LandSize" "BuildingArea"]))
      g/collect))
; => ({:Car 0.004565537555228277,
;      :LandSize 0.0,
;      :BuildingArea 0.47496318114874814})
```

## MLlib

The following examples are taken from [Apache Spark's MLlib guide](https://spark.apache.org/docs/latest/ml-guide.html).

### Basic Statistics

#### Correlation

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

#### Hypothesis Testing

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

### Features

#### Tokeniser, Hashing TF and IDF

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

#### PCA

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

#### Standard Scaler

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

#### Vector Assembler

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

### Classification

#### Logistic Regression

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

(take 3 (ml/coefficients lr-model))
; => (-7.353983524188197E-5 -9.102738505589466E-5 -1.9467430546904298E-4)

(ml/intercept lr-model)
; => 0.22456315961250325
```

#### Gradient Boosted Tree Classifier

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
; Test error: 0.08823529411764708
```

#### XGBoost Classifier

```clojure
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def xgb-model
  (ml/fit
    training
    (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-model)
    (g/select "label" "probability")
    (g/limit 5)
    g/show)
; +-----+----------------------------------------+
; |label|probability                             |
; +-----+----------------------------------------+
; |0.0  |[0.7502040266990662,0.24979597330093384]|
; |1.0  |[0.24869805574417114,0.7513019442558289]|
; |1.0  |[0.24869805574417114,0.7513019442558289]|
; |1.0  |[0.24869805574417114,0.7513019442558289]|
; |1.0  |[0.24869805574417114,0.7513019442558289]|
; +-----+----------------------------------------+
```

### Regression

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

(take 3 (ml/coefficients lr-model))
; => (0.0 0.0 0.0)

(ml/intercept lr-model)
; => 0.57
```

#### Random Forest Regression

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

#### XGBoost Regressor

```clojure
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def xgb-model
  (ml/fit
    training
    (ml/xgboost-regressor {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-model)
    (g/select "label" "prediction")
    (g/limit 5)
    g/show)
; +-----+-------------------+
; |label|prediction         |
; +-----+-------------------+
; |0.0  |0.24979597330093384|
; |1.0  |0.7513019442558289 |
; |1.0  |0.7513019442558289 |
; |1.0  |0.7513019442558289 |
; |1.0  |0.7513019442558289 |
; +-----+-------------------+
```

### Clustering

#### K-Means

```clojure
(def dataset
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/k-means {:k 2 :seed 1})))

(def predictions
  (ml/transform dataset model))

(def silhoutte (ml/evaluate predictions (ml/clustering-evaluator {})))

(println "Silhouette with squared euclidean distance:" silhoutte)
(println "Cluster centers:" (ml/cluster-centers model))
; Silhouette with squared euclidean distance: 0.9997530305375207
; Cluster centers: ((0.1 0.1 0.1) (9.1 9.1 9.1))
```

#### LDA

```clojure
(def dataset
  (g/read-libsvm! spark "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/lda {:k 10 :max-iter 10})))

(println "log-likehood:" (.logLikelihood model dataset))
(println "log-perplexity" (.logPerplexity model dataset))
; log-likehood: -164.51762514834732
; log-perplexity 1.9869278399558856

(-> dataset
    (ml/transform model)
    (g/limit 2)
    (g/collect-col "topicDistribution"))
; => ((0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0)
;     (0.07701806399774133
;      0.07701821590017151
;      0.07701312686434603
;      0.07701770385981303
;      0.3068312384243969
;      0.0770491751796324
;      0.07701324197964737
;      0.07701452273708989
;      0.07701480342311406
;      0.07700990763404734))
```

### Collaborative Filtering

```clojure
(defonce ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       clojure.string/split-lines
       (map #(clojure.string/split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (g/records->dataset spark)))

(def model
  (ml/fit ratings-df (ml/als {:max-iter   5
                              :reg-param  0.01
                              :user-col   "user-id"
                              :item-col   "movie-id"
                              :rating-col "rating"})))

(.setColdStartStrategy model "drop")
(def predictions
  (ml/transform ratings-df model))

(def evaluator
  (ml/regression-evaluator {:metric-name    "rmse"
                            :label-col      "rating"
                            :prediction-col "prediction"}))

(println "Root-mean-square error:" (ml/evaluate predictions evaluator))
(-> (ml/recommend-users model 3)
    (g/limit 5)
    g/show)
(-> (ml/recommend-items model 3)
    (g/limit 5)
    g/show)
; Root-mean-square error: 0.29591909389846743
; +--------+--------------------------------------------------+
; |movie-id|recommendations                                   |
; +--------+--------------------------------------------------+
; |31      |[[12, 3.893104], [6, 3.0838614], [14, 2.9631455]] |
; |85      |[[16, 4.730368], [8, 4.6532993], [7, 3.7848458]]  |
; |65      |[[23, 4.732419], [14, 3.1167293], [25, 2.436222]] |
; |53      |[[22, 5.329093], [4, 4.733863], [24, 4.6916943]]  |
; |78      |[[25, 1.3145051], [23, 1.1761607], [26, 1.135325]]|
; +--------+--------------------------------------------------+
; +-------+--------------------------------------------------+
; |user-id|recommendations                                   |
; +-------+--------------------------------------------------+
; |28     |[[25, 5.689864], [92, 5.360779], [76, 5.1021585]] |
; |26     |[[51, 6.298293], [22, 5.4222317], [94, 5.2276535]]|
; |27     |[[18, 3.7351623], [7, 3.692539], [23, 3.3052857]] |
; |12     |[[46, 9.0876255], [17, 4.984369], [35, 4.9596915]]|
; |22     |[[53, 5.329093], [74, 5.013483], [75, 4.916749]]  |
; +-------+--------------------------------------------------+
```

### Model Selection and Tuning

```clojure
(def training
  (g/table->dataset
    spark
    [[0  "a b c d e spark"  1.0]
     [1  "b d"              0.0]
     [2  "spark f g h"      1.0]
     [3  "hadoop mapreduce" 0.0]
     [4  "b spark who"      1.0]
     [5  "g d a y"          0.0]
     [6  "spark fly"        1.0]
     [7  "was mapreduce"    0.0]
     [8  "e spark program"  1.0]
     [9  "a e c l"          0.0]
     [10 "spark compile"    1.0]
     [11 "hadoop software"  0.0]]
    [:id :text :label]))

(def hashing-tf
  (ml/hashing-tf {:input-col "words" :output-col "features"}))

(def logistic-reg
  (ml/logistic-regression {:max-iter 10}))

(def pipeline
  (ml/pipeline
    (ml/tokeniser {:input-col "text" :output-col "words"})
    hashing-tf
    logistic-reg))

(def param-grid
  (ml/param-grid
    {hashing-tf {:num-features (mapv int [10 100 1000])}
     logistic-reg {:reg-param [0.1 0.01]}}))

(def cross-validator
  (ml/cross-validator {:estimator pipeline
                       :evaluator (ml/binary-classification-evaluator {})
                       :estimator-param-maps param-grid
                       :num-folds 2
                       :parallelism 2}))

(def cv-model (ml/fit training cross-validator))

(def testing
  (g/table->dataset
    spark
    [[4 "spark i j k"]
     [5 "l m n"]
     [6 "mapreduce spark"]
     [7 "apache hadoop"]]
    [:id :text]))

(-> testing
    (ml/transform cv-model)
    (g/select "id" "text" "probability" "prediction")
    g/collect)
; => ({:id 4,
;      :text "spark i j k",
;      :probability (0.12566260711357555 0.8743373928864244),
;      :prediction 1.0}
;     {:id 5,
;      :text "l m n",
;      :probability (0.995215441016286 0.004784558983713945),
;      :prediction 0.0}
;     {:id 6,
;      :text "mapreduce spark",
;      :probability (0.3069689523262689 0.693031047673731),
;      :prediction 1.0}
;     {:id 7,
;      :text "apache hadoop",
;      :probability (0.8040279442401511 0.19597205575984883),
;      :prediction 0.0})
```

### Frequent Pattern Mining

```clojure
(def dataset
  (-> (g/table->dataset
        spark
        [['("1" "2" "5")]
         ['("1" "2" "3" "5")]
         ['("1" "2")]]
        [:items])))

(def model
  (ml/fit
    dataset
    (ml/fp-growth {:items-col      "items"
                   :min-confidence 0.6
                   :min-support    0.5})))


(g/show (ml/frequent-item-sets model))
; +---------+----+
; |items    |freq|
; +---------+----+
; |[1]      |3   |
; |[2]      |3   |
; |[2, 1]   |3   |
; |[5]      |2   |
; |[5, 2]   |2   |
; |[5, 2, 1]|2   |
; |[5, 1]   |2   |
; +---------+----+

(g/show (ml/association-rules model))
; +----------+----------+------------------+----+
; |antecedent|consequent|confidence        |lift|
; +----------+----------+------------------+----+
; |[2, 1]    |[5]       |0.6666666666666666|1.0 |
; |[5, 1]    |[2]       |1.0               |1.0 |
; |[2]       |[1]       |1.0               |1.0 |
; |[2]       |[5]       |0.6666666666666666|1.0 |
; |[5]       |[2]       |1.0               |1.0 |
; |[5]       |[1]       |1.0               |1.0 |
; |[1]       |[2]       |1.0               |1.0 |
; |[1]       |[5]       |0.6666666666666666|1.0 |
; |[5, 2]    |[1]       |1.0               |1.0 |
; +----------+----------+------------------+----+
```
