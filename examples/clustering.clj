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
(println "Cluster centers:" (ml/cluster-centers model))

;; LDA
(def dataset
  (g/read-libsvm! "test/resources/sample_kmeans_data.txt"))

(def model
  (ml/fit dataset (ml/lda {:k 10 :max-iter 10})))

(println "log-likehood:" (ml/log-likelihood dataset model))
(println "log-perplexity" (ml/log-perplexity dataset model))

(-> dataset
    (ml/transform model)
    (g/limit 2)
    (g/collect-col :topicDistribution))
