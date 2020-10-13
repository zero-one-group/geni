(ns examples.recommendation
  (:require
   [clojure.string]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(defonce ratings-df
  (->> (slurp "test/resources/sample_movielens_ratings.txt")
       clojure.string/split-lines
       (map #(clojure.string/split % #"::"))
       (map (fn [row]
              {:user-id   (Integer/parseInt (first row))
               :movie-id  (Integer/parseInt (second row))
               :rating    (Float/parseFloat (nth row 2))
               :timestamp (long (Integer/parseInt (nth row 3)))}))
       (g/records->dataset)))

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
;;=> Root-mean-square error: 0.26560202101375274

(-> (ml/recommend-users model 3)
    (g/limit 5)
    g/show)

;;=>
;; +--------+---------------------------------------------------+
;; |movie-id|recommendations                                    |
;; +--------+---------------------------------------------------+
;; |20      |[[17, 4.6264486], [23, 3.389515], [5, 3.362232]]   |
;; |40      |[[10, 3.9663665], [2, 3.7211847], [28, 3.1287851]] |
;; |31      |[[12, 3.59246], [16, 3.400046], [6, 3.3980522]]    |
;; |81      |[[28, 4.9357247], [11, 4.070512], [27, 3.25047]]   |
;; |91      |[[17, 2.9520838], [12, 2.9241128], [28, 2.8053453]]|
;; +--------+---------------------------------------------------+

(-> (ml/recommend-items model 3)
    (g/limit 5)
    g/show)

;;=>
;; +-------+---------------------------------------------------+
;; |user-id|recommendations                                    |
;; +-------+---------------------------------------------------+
;; |1      |[[22, 3.7155223], [62, 3.6570792], [68, 3.6425865]]|
;; |12     |[[46, 6.078253], [90, 5.763729], [17, 5.0272517]]  |
;; |22     |[[75, 5.1721573], [88, 5.0102735], [51, 4.995222]] |
;; |13     |[[93, 3.6989255], [29, 3.1743684], [96, 2.925599]] |
;; |26     |[[30, 6.733059], [32, 5.3192105], [22, 5.2058535]] |
;; +-------+---------------------------------------------------+
