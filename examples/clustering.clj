(ns examples.clustering
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

;; K-Means
(def dataset
  (g/read-libsvm! "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/k-means {:k 2 :seed 1})))

(def predictions
  (ml/transform dataset model))

(def silhoutte (ml/evaluate predictions (ml/clustering-evaluator {})))

(println "Silhouette with squared euclidean distance:" silhoutte)
;;=> Silhouette with squared euclidean distance: 0.9997530305375207

(println "Cluster centers:" (ml/cluster-centers model))
;;=> Cluster centers: ((9.1 9.1 9.1) (0.1 0.1 0.1))

;; LDA
(def dataset
  (g/read-libsvm! "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/lda {:k 10 :max-iter 10})))

(println "log-likehood:" (ml/log-likelihood dataset model))
;;=> log-likehood: -136.8259177878647

(println "log-perplexity" (ml/log-perplexity dataset model))
;;=> log-perplexity 1.6524869298051295

(-> dataset
    (ml/transform model)
    (g/limit 2)
    (g/collect-col :topicDistribution))
#_
((0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0)
 (0.07537574948606664
  0.07537608757379946
  0.07537444990446238
  0.0753760406513953
  0.32162508215023483
  0.07537597087758716
  0.0753743398006919
  0.0753743698147718
  0.075374842014245
  0.07537306772674557))
