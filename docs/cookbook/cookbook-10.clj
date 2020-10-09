(ns geni.cookbook-10
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(load-file "docs/cookbook/cookbook-util.clj")

;; Part 10: Avoiding Repeated Computations with Caching

(def dummy-data-path "data/performance-benchmark-data")

(def transactions (g/read-parquet! dummy-data-path))

(g/count transactions)

(g/print-schema transactions)

;;=>
;; root
;; |-- member-id: integer (nullable = true)
;; |-- day: long (nullable = true)
;; |-- trx-id: string (nullable = true)
;; |-- brand-id: integer (nullable = true)
;; |-- month: long (nullable = true)
;; |-- year: long (nullable = true)
;; |-- quantity: integer (nullable = true)
;; |-- price: double (nullable = true)
;; |-- style-id: integer (nullable = true)
;; |-- date: date (nullable = true)

;; 10.1 Putting Together A Member Profile

(def member-spending
  (-> transactions
      (g/with-column :sales (g/* :price :quantity))
      (g/group-by :member-id)
      (g/agg {:total-spend     (g/sum :sales)
              :avg-basket-size (g/mean :sales)
              :avg-price       (g/mean :price)})))

(def member-frequency
  (-> transactions
      (g/group-by :member-id)
      (g/agg {:n-transactions (g/count "*")
              :n-visits       (g/count-distinct :date)})))

(def member-profile
  (g/join member-spending member-frequency :member-id))

(g/print-schema member-profile)

;;=>
;; root
;; |-- member-id: integer (nullable = true)
;; |-- total-spend: double (nullable = true)
;; |-- avg-basket-size: double (nullable = true)
;; |-- avg-price: double (nullable = true)
;; |-- n-transactions: long (nullable = false)
;; |-- n-visits: long (nullable = false)

;; 10.2 Caching Intermediate Results

(defn some-other-computations [member-profile]
  (g/write-parquet! member-profile "data/temp.parquet" {:mode "overwrite"}))

(doall (for [_ (range 5)]
         (time (some-other-computations member-profile))))

;; "Elapsed time: 10083.047244 msecs"
;; "Elapsed time: 8231.45662 msecs"
;; "Elapsed time: 8525.947692 msecs"
;; "Elapsed time: 8155.982435 msecs"
;; "Elapsed time: 7638.144858 msecs"

(def cached-member-profile
  (g/cache member-profile))

(doall (for [_ (range 5)]
         (time (some-other-computations cached-member-profile))))
;; "Elapsed time: 11996.307581 msecs"
;; "Elapsed time: 988.958567 msecs"
;; "Elapsed time: 1017.365143 msecs"
;; "Elapsed time: 1032.578846 msecs"
;; "Elapsed time: 1087.077004 msecs"

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

;; Part 12: Customer Segmentation with NMF
(def invoices
  (g/read-csv! "data/online_retail_ii" {:kebab-columns true}))

(g/print-schema invoices)

;;=>
;; root
;;  |-- invoice: string (nullable = true)
;;  |-- stock-code: string (nullable = true)
;;  |-- description: string (nullable = true)
;;  |-- quantity: integer (nullable = true)
;;  |-- invoice-date: string (nullable = true)
;;  |-- price: double (nullable = true)
;;  |-- customer-id: integer (nullable = true)
;;  |-- country: string (nullable = true)

(g/count invoices)
;;=> 1067371

(-> invoices (g/limit 2) g/show-vertical)
;;=>
;; -RECORD 0-------------------------------------------
;;  invoice      | 489434
;;  stock-code   | 85048
;;  description  | 15CM CHRISTMAS GLASS BALL 20 LIGHTS
;;  quantity     | 12
;;  invoice-date | 1/12/2009 07:45
;;  price        | 6.95
;;  customer-id  | 13085
;;  country      | United Kingdom
;; -RECORD 1-------------------------------------------
;;  invoice      | 489434
;;  stock-code   | 79323P
;;  description  | PINK CHERRY LIGHTS
;;  quantity     | 12
;;  invoice-date | 1/12/2009 07:45
;;  price        | 6.75
;;  customer-id  | 13085
;;  country      | United Kingdom

;; 12.1 Exploding Sentences into Words

(def descriptors
  (-> invoices
      (g/remove (g/null? :description))
      (ml/transform
       (ml/tokeniser {:input-col  :description
                      :output-col :descriptors}))
      (ml/transform
       (ml/stop-words-remover {:input-col  :descriptors
                               :output-col :cleaned-descriptors}))
      (g/with-column :descriptor (g/explode :cleaned-descriptors))
      (g/with-column :descriptor (g/regexp-replace :descriptor
                                                   (g/lit "[^a-zA-Z'']")
                                                   (g/lit "")))
      (g/remove (g/< (g/length :descriptor) 3))
      g/cache))

(-> descriptors
    (g/group-by :descriptor)
    (g/agg {:total-spend (g/int (g/sum (g/* :price :quantity)))})
    (g/sort (g/desc :total-spend))
    (g/limit 10)
    g/show)

;;=>
;; +----------+-----------+
;; |descriptor|total-spend|
;; +----------+-----------+
;; |set       |2089125    |
;; |bag       |1912097    |
;; |red       |1834692    |
;; |heart     |1465429    |
;; |vintage   |1179526    |
;; |retrospot |1166847    |
;; |white     |1155863    |
;; |pink      |1009384    |
;; |jumbo     |984806     |
;; |design    |917394     |
;; +----------+-----------+

(-> descriptors (g/select :descriptor) g/distinct g/count)
;;=> 2605

;; 12.2 Non-Negative Matrix Factorisation

(def log-spending
  (-> descriptors
      (g/remove (g/||
                 (g/null? :customer-id)
                 (g/< :price 0.01)
                 (g/< :quantity 1)))
      (g/group-by :customer-id :descriptor)
      (g/agg {:log-spend (g/log1p (g/sum (g/* :price :quantity)))})
      (g/order-by (g/desc :log-spend))))

(-> log-spending (g/describe :log-spend) g/show)

;;=>
;; +-------+--------------------+
;; |summary|log-spend           |
;; +-------+--------------------+
;; |count  |837985              |
;; |mean   |3.173295903226327   |
;; |stddev |1.3183533551300999  |
;; |min    |0.058268908123975775|
;; |max    |12.034516532838857  |
;; +-------+--------------------+

(def nmf-pipeline
  (ml/pipeline
   (ml/string-indexer {:input-col  :descriptor
                       :output-col :descriptor-id})
   (ml/als {:max-iter    100
            :reg-param   0.01
            :rank        8
            :nonnegative true
            :user-col    :customer-id
            :item-col    :descriptor-id
            :rating-col  :log-spend})))

(def nmf-pipeline-model
  (ml/fit log-spending nmf-pipeline))

;; 12.3 Linking Segments with Members and Descriptors

(def id->descriptor
  (ml/index-to-string
   {:input-col  :id
    :output-col :descriptor
    :labels     (ml/labels (first (ml/stages nmf-pipeline-model)))}))

(def nmf-model (last (ml/stages nmf-pipeline-model)))

(def shared-patterns
  (-> (ml/item-factors nmf-model)
      (ml/transform id->descriptor)
      (g/select :descriptor (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column
        :pattern-rank
        (g/windowed {:window-col   (g/row-number)
                     :partition-by :pattern-id
                     :order-by     (g/desc :factor-weight)}))
      (g/filter (g/< :pattern-rank 6))
      (g/order-by :pattern-id (g/desc :factor-weight))
      (g/select :pattern-id :descriptor :factor-weight)))

(-> shared-patterns
    (g/group-by :pattern-id)
    (g/agg {:descriptors (g/array-sort (g/collect-set :descriptor))})
    (g/order-by :pattern-id)
    g/show)

;;=>
;; +----------+----------------------------------------------------------+
;; |pattern-id|descriptors                                               |
;; +----------+----------------------------------------------------------+
;; |0         |[heart, holder, jun, peter, tlight]                       |
;; |1         |[bar, draw, garld, seventeen, sideboard]                  |
;; |2         |[coathangers, jun, peter, pinkblack, rucksack]            |
;; |3         |[bag, jumbo, lunch, red, retrospot]                       |
;; |4         |[retrodisc, rnd, scissor, sculpted, shapes]               |
;; |5         |[afghan, capiz, lazer, mugcoasterlavender, yellowblue]    |
;; |6         |[cake, metal, sign, stand, time]                          |
;; |7         |[mintivory, necklturquois, pinkamethystgold, regency, set]|
;; +----------+----------------------------------------------------------+

(def customer-segments
  (-> (ml/user-factors nmf-model)
      (g/select (g/as :id :customer-id) (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column
        :customer-rank
        (g/windowed {:window-col   (g/row-number)
                     :partition-by :customer-id
                     :order-by     (g/desc :factor-weight)}))
      (g/filter (g/= :customer-rank 1))))

(-> customer-segments
    (g/group-by :pattern-id)
    (g/agg {:n-customers (g/count-distinct :customer-id)})
    (g/order-by :pattern-id)
    g/show)

;;=>
;; +----------+-----------+
;; |pattern-id|n-customers|
;; +----------+-----------+
;; |0         |760        |
;; |1         |1095       |
;; |2         |379        |
;; |3         |444        |
;; |4         |1544       |
;; |5         |756        |
;; |6         |426        |
;; |7         |474        |
;; +----------+-----------+
