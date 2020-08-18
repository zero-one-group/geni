(ns zero-one.geni.aot-functions
  (:require
    [clojure.string]))

(defn to-pair [x] [x 1])

(defn split-spaces [x] (clojure.string/split x #" "))

(defn equals-lewis [x] (= x "Lewis"))
