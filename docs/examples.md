# Geni Examples

The examples use the datasets in Geni's repo, under `test/resources`:

```clojure
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(def melbourne-df (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet"))

(def libsvm-df (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))
```

## Dataframe API

The following examples are taken from [Apache Spark's example page](https://spark.apache.org/examples.html) and [Databricks' examples](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html).

### Text Search

```clojure
(-> melbourne-df
    (g/filter (g/like :Suburb "%South%"))
    (g/select "Suburb"
              (g/lower "SellerG")
              :Regionname
              (g/upper :Type))
    g/distinct
    (g/limit 5)
    g/show)
;; =stdout=>
; +---------------+--------------+--------------------------+-----------+
; |Suburb         |lower(SellerG)|Regionname                |upper(Type)|
; +---------------+--------------+--------------------------+-----------+
; |Wantirna South |llc           |Eastern Metropolitan      |H          |
; |South Melbourne|conquest      |Southern Metropolitan     |H          |
; |Frankston South|ray           |South-Eastern Metropolitan|H          |
; |South Melbourne|cayzer        |Southern Metropolitan     |U          |
; |South Melbourne|williams      |Southern Metropolitan     |H          |
; +---------------+--------------+--------------------------+-----------+
```

### Printing Schema

```clojure
(-> melbourne-df
    (g/select :Suburb :Rooms :Price)
    g/print-schema)
;; =stdout=>
; root
;  |-- Suburb: string (nullable = true)
;  |-- Rooms: long (nullable = true)
;  |-- Price: double (nullable = true)
```

### Descriptive Statistics

```clojure
(-> melbourne-df
    (g/describe :Price)
    g/show)
;; =stdout=>
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
(-> melbourne-df
    (g/agg {:car           (g/null-rate :Car)
           :land-size     (g/null-rate :LandSize)
           :building-area (g/null-rate :BuildingArea)})
    g/collect)
;; => ({:car 0.004565537555228277, :land-size 0.0, :building-area 0.47496318114874814})
```

### Window Functions

Every window spec is used with `over` at some point. Use the `windowed` shortcut:

```clojure
(-> melbourne-df
    (g/select {:seller :SellerG
               :price  :Price
               :ranks  (g/over (g/rank)
                               (g/window {:partition-by :SellerG
                                          :order-by     (g/desc :Price)}))})
    (g/filter (g/= :ranks 1))
    (g/limit 5)
    g/show)
;; =stdout=>
; +------------+---------+-----+
; |seller      |price    |ranks|
; +------------+---------+-----+
; |@Realty     |725000.0 |1    |
; |ASL         |1890000.0|1    |
; |Abercromby's|7650000.0|1    |
; |Ace         |860000.0 |1    |
; |Alexkarbon  |2370000.0|1    |
; +------------+---------+-----+

(-> melbourne-df
    (g/select {:seller :SellerG
               :price  :Price
               :ranks  (g/windowed {:window-col (g/rank)
                                    :partition-by :SellerG
                                    :order-by     (g/desc :Price)})})
    (g/filter (g/= :ranks 1))
    (g/limit 5)
    g/show)
;; =stdout=>
; +------------+---------+-----+
; |seller      |price    |ranks|
; +------------+---------+-----+
; |@Realty     |725000.0 |1    |
; |ASL         |1890000.0|1    |
; |Abercromby's|7650000.0|1    |
; |Ace         |860000.0 |1    |
; |Alexkarbon  |2370000.0|1    |
; +------------+---------+-----+
```

## MLlib

The following examples are taken from [Apache Spark's MLlib guide](https://spark.apache.org/docs/latest/ml-guide.html).

### Basic Statistics

#### Correlation

```clojure
(def corr-df
  (g/table->dataset
    [[(g/dense 1.0 0.0 -2.0 0.0)]
     [(g/dense 4.0 5.0 0.0  3.0)]
     [(g/dense 6.0 7.0 0.0  8.0)]
     [(g/dense 9.0 0.0 1.0  0.0)]]
    [:features]))

(let [corr-kw (keyword "pearson(features)")]
  (corr-kw (g/first (ml/corr corr-df :features))))
;; => ((1.0 0.05564148840746571 0.9442673704375605 0.13114824589410562)
;;     (0.05564148840746571 1.0 0.2232968782694361 0.9428090415820634)
;;     (0.9442673704375605 0.2232968782694361 1.0 0.19298245614035087)
;;     (0.13114824589410562 0.9428090415820634 0.19298245614035087 1.0))
```

#### Hypothesis Testing

```clojure
(def hypothesis-df
  (g/table->dataset
     [[0.0 (g/dense 0.5 10.0)]
      [0.0 (g/dense 1.5 20.0)]
      [1.0 (g/dense 1.5 30.0)]
      [0.0 (g/dense 3.5 30.0)]
      [0.0 (g/dense 3.5 40.0)]
      [1.0 (g/dense 3.5 40.0)]]
     [:label :features]))

(g/first (ml/chi-square-test hypothesis-df :features :label))
;; => {:pValues (0.6872892787909721 0.6822703303362126),
;;     :degreesOfFreedom (2 3),
;;     :statistics (0.75 1.5)}
```

### Features

#### Tokeniser, Hashing TF and IDF

```clojure
(def sentence-data
  (g/table->dataset
    [[0.0 "Hi I heard about Spark"]
     [0.0 "I wish Java could use case classes"]
     [1.0 "Logistic regression models are neat"]]
    [:label :sentence]))

(def pipeline
  (ml/pipeline
    (ml/tokenizer {:input-col :sentence
                    :output-col :words})
    (ml/hashing-tf {:num-features 20
                    :input-col :words
                    :output-col :raw-features})
    (ml/idf {:input-col :raw-features
              :output-col :features})))

(def pipeline-model
  (ml/fit sentence-data pipeline))

(-> sentence-data
    (ml/transform pipeline-model)
    (g/collect-col :features))
    
;; => ({:size 20,
;;      :indices (6 8 13 16),
;;      :values
;;      (0.28768207245178085
;;       0.6931471805599453
;;       0.28768207245178085
;;       0.5753641449035617)}
;;     {:size 20,
;;      :indices (0 2 7 13 15 16),
;;      :values
;;      (0.6931471805599453
;;       0.6931471805599453
;;       1.3862943611198906
;;       0.28768207245178085
;;       0.6931471805599453
;;       0.28768207245178085)}
;;     {:size 20,
;;      :indices (3 4 6 11 19),
;;      :values
;;      (0.6931471805599453
;;       0.6931471805599453
;;       0.28768207245178085
;;       0.6931471805599453
;;       0.6931471805599453)})
```

#### PCA

```clojure
(def dataframe
  (g/table->dataset
    [[(g/dense 0.0 1.0 0.0 7.0 0.0)]
     [(g/dense 2.0 0.0 3.0 4.0 5.0)]
     [(g/dense 4.0 0.0 0.0 6.0 7.0)]]
    [:features]))

(def pca
  (ml/fit dataframe (ml/pca {:input-col :features
                             :output-col :pca-features
                             :k 2})))

(-> dataframe
    (ml/transform pca)
    (g/collect-col :pca-features))

;; => ((1.6485728230883814 -4.013282700516299)
;;     (-4.645104331781533 -1.116797266361906)
;;     (-6.428880535676489 -5.33795142777536))
```

#### Standard Scaler

```clojure
(def scaler
  (ml/standard-scaler {:input-col :features
                       :output-col :scaled-features
                       :with-std true
                       :with-mean false}))

(def scaler-model (ml/fit libsvm-df scaler))

(def scaled-features
  (-> libsvm-df
      (ml/transform scaler-model)
      (g/limit 1)
      (g/collect-col :scaled-features)
      first))

(:size scaled-features)
;; => 692

(take 5 (:values scaled-features))
;; => (0.5468234998110156 1.5923262059067456 2.435399721310935
;;     1.7081091742536456 0.7334796787587756)
```

#### Vector Assembler

```clojure
(def dataset
  (g/table->dataset
    [[0 18 1.0 (g/dense 0.0 10.0 0.5) 1.0]]
    [:id :hour :mobile :user-features :clicked]))

(def assembler
  (ml/vector-assembler {:input-cols [:hour :mobile :user-features]
                        :output-col :features}))

(-> dataset
    (ml/transform assembler)
    (g/select :features :clicked)
    g/show)
;; =stdout=>
; +-----------------------+-------+
; |features               |clicked|
; +-----------------------+-------+
; |[18.0,1.0,0.0,10.0,0.5]|1.0    |
; +-----------------------+-------+
```

### Classification

#### Logistic Regression

```clojure
(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def lr (ml/logistic-regression {:max-iter 10
                                 :reg-param 0.3
                                 :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select :label :probability)
    (g/limit 5)
    g/show)
;; =stdout=>
; +-----+----------------------------------------+
; |label|probability                             |
; +-----+----------------------------------------+
; |0.0  |[0.7151376213452819,0.2848623786547181] |
; |1.0  |[0.22557925799292566,0.7744207420070743]|
; |1.0  |[0.21174734149915936,0.7882526585008407]|
; |1.0  |[0.28335873342117446,0.7166412665788255]|
; |1.0  |[0.2354815309564311,0.7645184690435689] |
; +-----+----------------------------------------+

(count (ml/coefficients lr-model))
;; => 692

(->> (ml/coefficients lr-model) (remove zero?) (take 3))
;; => (-7.520689871383902E-5 -8.11577314684679E-5 3.814692771846554E-5)

(ml/intercept lr-model)
;; => -0.5991460286401467
```

#### Gradient Boosted Tree Classifier

```clojure
(def data (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def split-data (g/random-split data [0.7 0.3] 1234))
(def train-data (first split-data))
(def test-data (second split-data))

(def label-indexer
  (ml/fit data (ml/string-indexer {:input-col :label :output-col :indexed-label})))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col :features
                                   :output-col :indexed-features
                                   :max-categories 4})))

(def pipeline
  (ml/pipeline
    label-indexer
    feature-indexer
    (ml/gbt-classifier {:label-col :indexed-label
                        :features-col :indexed-features
                        :max-iter 10
                        :feature-subset-strategy "auto"})
    (ml/index-to-string {:input-col :prediction
                         :output-col :predicted-label
                         :labels (ml/labels label-indexer)})))

(def model (ml/fit train-data pipeline))

(def predictions (ml/transform test-data model))

(def evaluator
  (ml/multiclass-classification-evaluator {:label-col :indexed-label
                                           :prediction-col :prediction
                                           :metric-name "accuracy"}))

(-> predictions
    (g/select :predicted-label :label)
    (g/limit 5)
    g/show)
;; =stdout=>
; +---------------+-----+
; |predicted-label|label|
; +---------------+-----+
; |0.0            |0.0  |
; |0.0            |0.0  |
; |0.0            |0.0  |
; |0.0            |0.0  |
; |0.0            |0.0  |
; +---------------+-----+

(println "Test error:" (- 1 (ml/evaluate predictions evaluator)))
;; =stdout=>
; Test error: 0.0
```

#### XGBoost Classifier

[Optional XGBoost support](xgboost.md) has an example, which needs XGBoost4J-Spark on the classpath.

### Regression

#### Linear Regression

```clojure
(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def lr (ml/linear-regression {:max-iter 10
                               :reg-param 0.8
                               :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select :label :prediction)
    (g/limit 5)
    g/show)
;; =stdout=>
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
;; => (0.0 0.0 0.0)

(ml/intercept lr-model)
;; => 0.57
```

#### Random Forest Regression

```clojure
(def data (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col :features
                                   :output-col :indexed-features
                                   :max-categories 4})))

(def split-data (g/random-split data [0.7 0.3] 1234))
(def train-data (first split-data))
(def test-data (second split-data))

(def pipeline
  (ml/pipeline
    feature-indexer
    (ml/random-forest-regressor {:label-col :label
                                 :features-col :indexed-features})))

(def model (ml/fit train-data pipeline))
(def predictions (ml/transform test-data model))
(def evaluator
  (ml/regression-evaluator {:label-col :label
                            :prediction-col :prediction
                            :metric-name "rmse"}))

(-> predictions
    (g/select :prediction :label)
    (g/show {:num-rows 5}))
;; =stdout=>
; +----------+-----+
; |prediction|label|
; +----------+-----+
; |0.0       |0.0  |
; |0.0       |0.0  |
; |0.0       |0.0  |
; |0.0       |0.0  |
; |0.0       |0.0  |
; +----------+-----+
; only showing top 5 rows

(println "RMSE:" (ml/evaluate predictions evaluator))
;; =stdout=>
; RMSE: 0.04767312946227961
```

#### Survival Regression

```clojure
(def train
  (g/table->dataset
    [[1.218 1.0 (g/dense 1.560 -0.605)]
     [2.949 0.0 (g/dense 0.346  2.158)]
     [3.627 0.0 (g/dense 1.380  0.231)]
     [0.273 1.0 (g/dense 0.520  1.151)]
     [4.199 0.0 (g/dense 0.795 -0.226)]]
    [:label :censor :features]))

(def quantile-probabilities [0.3 0.6])

(def aft
  (ml/aft-survival-regression
    {:quantile-probabilities quantile-probabilities
     :quantiles-col :quantiles}))

(def aft-model (ml/fit train aft))

(-> train (ml/transform aft-model) g/show)
;; =stdout=>
; +-----+------+--------------+------------------+---------------------------------------+
; |label|censor|features      |prediction        |quantiles                              |
; +-----+------+--------------+------------------+---------------------------------------+
; |1.218|1.0   |[1.56,-0.605] |5.7189965530299   |[1.1603295951029091,4.995471733719646] |
; |2.949|0.0   |[0.346,2.158] |18.076458028588927|[3.6675401061563924,15.78955928549122] |
; |3.627|0.0   |[1.38,0.231]  |7.381875365763504 |[1.4977117707333796,6.447975512763028] |
; |0.273|1.0   |[0.52,1.151]  |13.577581299077902|[2.7547611307597735,11.859846908963423]|
; |4.199|0.0   |[0.795,-0.226]|9.013093216625728 |[1.8286702406091537,7.872823838856878] |
; +-----+------+--------------+------------------+---------------------------------------+
```

#### XGBoost Regressor

[Optional XGBoost support](xgboost.md) has an example, which needs XGBoost4J-Spark on the classpath.

### Clustering

#### K-Means

```clojure
(def dataset
  (g/read-libsvm! "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/k-means {:k 2 :seed 1})))

(def predictions
  (ml/transform dataset model))

(def silhoutte (ml/evaluate predictions (ml/clustering-evaluator {})))

(println "Silhouette with squared euclidean distance:" silhoutte)
;; =stdout=>
; Silhouette with squared euclidean distance: 0.9997530305375207

(println "Cluster centers:" (ml/cluster-centers model))
;; =stdout=>
; Cluster centers: ((9.1 9.1 9.1) (0.1 0.1 0.1))
```

#### LDA

```clojure
(def dataset
  (g/read-libsvm! "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/lda {:k 10 :max-iter 10})))

(println "log-likelihood:" (.logLikelihood model dataset))
;; =stdout=>
; log-likelihood: -136.8259177878647

(println "log-perplexity:" (.logPerplexity model dataset))
;; =stdout=>
; log-perplexity: 1.6524869298051295

(-> dataset
    (ml/transform model)
    (g/limit 2)
    (g/collect-col :topicDistribution))
;; => ((0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0)
;;     (0.07537574948606664
;;      0.07537608757379946
;;      0.07537444990446238
;;      0.0753760406513953
;;      0.3216250821502348
;;      0.07537597087758716
;;      0.0753743398006919
;;      0.0753743698147718
;;      0.075374842014245
;;      0.07537306772674557))
```

### Collaborative Filtering

```clojure
(def ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       clojure.string/split-lines
       (map #(clojure.string/split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       g/records->dataset))

(def model
  (ml/fit ratings-df (ml/als {:max-iter   5
                              :reg-param  0.01
                              :user-col   :user-id
                              :item-col   :movie-id
                              :rating-col :rating})))

(.setColdStartStrategy model "drop")
(def predictions
  (ml/transform ratings-df model))

(def evaluator
  (ml/regression-evaluator {:metric-name    "rmse"
                            :label-col      :rating
                            :prediction-col :prediction}))

(println "Root-mean-square error:" (ml/evaluate predictions evaluator))
;; =stdout=>
; Root-mean-square error: 0.2656020220314336

(-> (ml/recommend-users model 3)
    (g/limit 5)
    g/show)
;; =stdout=>
; +--------+---------------------------------------------------+
; |movie-id|recommendations                                    |
; +--------+---------------------------------------------------+
; |20      |[{17, 4.626449}, {23, 3.3895144}, {5, 3.3622315}]  |
; |40      |[{10, 3.9663663}, {2, 3.721185}, {28, 3.1287856}]  |
; |10      |[{17, 3.9731963}, {23, 3.6774054}, {12, 3.0596843}]|
; |50      |[{12, 4.1866217}, {23, 4.0086837}, {11, 3.8635526}]|
; |80      |[{3, 3.9520073}, {11, 3.3872929}, {22, 3.1267433}] |
; +--------+---------------------------------------------------+

(-> (ml/recommend-items model 3)
    (g/limit 5)
    g/show)
;; =stdout=>
; +-------+---------------------------------------------------+
; |user-id|recommendations                                    |
; +-------+---------------------------------------------------+
; |20     |[{22, 4.613506}, {68, 3.9787068}, {77, 3.7406487}] |
; |10     |[{85, 5.02638}, {32, 4.039686}, {40, 3.9663663}]   |
; |0      |[{25, 4.1894135}, {92, 3.7078776}, {9, 3.6283653}] |
; |1      |[{22, 3.7155223}, {62, 3.6570795}, {68, 3.6425867}]|
; |21     |[{29, 5.0556}, {52, 4.73343}, {53, 4.7176776}]     |
; +-------+---------------------------------------------------+
```

### Model Selection and Tuning

```clojure
(def training
  (g/table->dataset
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
  (ml/hashing-tf {:input-col :words :output-col :features}))

(def logistic-reg
  (ml/logistic-regression {:max-iter 10}))

(def pipeline
  (ml/pipeline
    (ml/tokeniser {:input-col :text :output-col :words})
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
    [[4 "spark i j k"]
     [5 "l m n"]
     [6 "mapreduce spark"]
     [7 "apache hadoop"]]
    [:id :text]))

(-> testing
    (ml/transform cv-model)
    (g/select :id :text :probability :prediction)
    g/collect)
;; => ({:id 4,
;;      :text "spark i j k",
;;      :probability (0.2664764605192917 0.7335235394807083),
;;      :prediction 1.0}
;;     {:id 5,
;;      :text "l m n",
;;      :probability (0.9203725758458563 0.0796274241541437),
;;      :prediction 0.0}
;;     {:id 6,
;;      :text "mapreduce spark",
;;      :probability (0.44376360608061827 0.5562363939193817),
;;      :prediction 1.0}
;;     {:id 7,
;;      :text "apache hadoop",
;;      :probability (0.8586524968002056 0.1413475031997944),
;;      :prediction 0.0})
```

### Frequent Pattern Mining

```clojure
(def dataset
  (-> (g/table->dataset
        [['("1" "2" "5")]
         ['("1" "2" "3" "5")]
         ['("1" "2")]]
        [:items])))

(def model
  (ml/fit
    dataset
    (ml/fp-growth {:items-col      :items
                   :min-confidence 0.6
                   :min-support    0.5})))


(g/show (ml/frequent-item-sets model))
;; =stdout=>
; +---------+----+
; |items    |freq|
; +---------+----+
; |[5]      |2   |
; |[5, 1]   |2   |
; |[5, 1, 2]|2   |
; |[5, 2]   |2   |
; |[2]      |3   |
; |[1]      |3   |
; |[1, 2]   |3   |
; +---------+----+

(g/show (ml/association-rules model))
;; =stdout=>
; +----------+----------+------------------+----+------------------+
; |antecedent|consequent|confidence        |lift|support           |
; +----------+----------+------------------+----+------------------+
; |[2]       |[5]       |0.6666666666666666|1.0 |0.6666666666666666|
; |[2]       |[1]       |1.0               |1.0 |1.0               |
; |[5, 2]    |[1]       |1.0               |1.0 |0.6666666666666666|
; |[1, 2]    |[5]       |0.6666666666666666|1.0 |0.6666666666666666|
; |[5, 1]    |[2]       |1.0               |1.0 |0.6666666666666666|
; |[5]       |[1]       |1.0               |1.0 |0.6666666666666666|
; |[5]       |[2]       |1.0               |1.0 |0.6666666666666666|
; |[1]       |[5]       |0.6666666666666666|1.0 |0.6666666666666666|
; |[1]       |[2]       |1.0               |1.0 |1.0               |
; +----------+----------+------------------+----+------------------+
```
