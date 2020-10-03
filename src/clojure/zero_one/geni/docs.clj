(ns zero-one.geni.docs
  (:require
    [clojure.java.io :as io]
    [taoensso.nippy :as nippy])
  (:import
    (org.apache.commons.io IOUtils)))

(def spark-docs
  (-> "spark-docs.nippy"
      io/resource
      io/input-stream
      IOUtils/toByteArray
      nippy/thaw))
