(ns zero-one.geni.ml-clustering
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.clustering BisectingKMeans
                                    GaussianMixture
                                    KMeans
                                    LDA)))

(defn bisecting-k-means [params]
  (let [defaults {:distance-measure "euclidean",
                  :max-iter 20,
                  :features-col "features",
                  :k 4,
                  :min-divisible-cluster-size 1.0,
                  :seed 566573821,
                  :prediction-col "prediction"}
        props     (merge defaults params)]
    (interop/instantiate BisectingKMeans props)))

(defn gaussian-mixture [params]
  (let [defaults {:seed 538009335,
                  :k 2,
                  :max-iter 100,
                  :probability-col "probability",
                  :tol 0.01,
                  :features-col "features",
                  :prediction-col "prediction"}
        props     (merge defaults params)]
    (interop/instantiate GaussianMixture props)))
(def gmm gaussian-mixture)

(defn k-means [params]
  (let [defaults {:max-iter         20,
                  :tol              1.0E-4,
                  :init-mode        "k-means||",
                  :seed             -1689246527,
                  :k                2,
                  :init-steps       2,
                  :distance-measure "euclidean",
                  :prediction-col   "prediction",
                  :features-col     "features"}
        props     (merge defaults params)]
    (interop/instantiate KMeans props)))

(defn lda [params]
  (let [defaults {:subsampling-rate 0.05,
                  :max-iter 20,
                  :keep-last-checkpoint true,
                  :topic-distribution-col "topicDistribution",
                  :optimize-doc-concentration true,
                  :seed 1435876747,
                  :k 10,
                  :learning-offset 1024.0,
                  :checkpoint-interval 10,
                  :optimizer "online",
                  :learning-decay 0.51,
                  :features-col "features"}
        props     (merge defaults params)]
    (interop/instantiate LDA props)))
(def latent-dirichlet-allocation lda)
