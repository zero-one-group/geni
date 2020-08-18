(ns zero-one.geni.aot-functions
  (:require
    [clojure.string]))

(defn to-pair [x] [x 1])

(defn split-spaces [x] (clojure.string/split x #" "))

(defn zip-split-spaces [xs ys]
  (map str
       (mapcat split-spaces (iterator-seq xs))
       (mapcat split-spaces (iterator-seq ys))))

(defn split-spaces-and-pair [x]
  (map #(vector % 1) (split-spaces x)))

(defn equals-lewis [x] (= x "Lewis"))
