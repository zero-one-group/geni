(ns geni.cookbook-12
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(load-file "docs/cookbook/cookbook-util.clj")

;; Part 12: Customer Segmentation with NMF

;; Note: need to get this from Kaggle - registration required
;; e.g. https://www.kaggle.com/hikne707/online-retail?select=online_retail_II.xlsx
(def invoices
  (g/read-csv! "data/online_retail_ii" {:kebab-columns true}))

(g/print-schema invoices)
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
;; 1067371

(-> invoices (g/limit 2) g/show-vertical)
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
;; => 2605

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
