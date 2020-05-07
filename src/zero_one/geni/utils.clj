(ns zero-one.geni.utils)

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))
