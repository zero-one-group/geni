(ns zero-one.geni.utils)

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn vector-of-numbers? [value]
  (and (vector? value) (every? number? value)))
