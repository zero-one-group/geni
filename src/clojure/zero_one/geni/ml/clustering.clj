(ns zero-one.geni.ml.clustering
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]]))

(interop/def-stages org.apache.spark.ml.clustering
  [bisecting-k-means BisectingKMeans]
  [gaussian-mixture GaussianMixture]
  [k-means KMeans]
  [lda LDA]
  [power-iteration-clustering PowerIterationClustering])

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.clustering
 [(-> docs/spark-docs :classes :ml :clustering)])

;; Aliases
(import-fn gaussian-mixture gmm)
(import-fn lda latent-dirichlet-allocation)
