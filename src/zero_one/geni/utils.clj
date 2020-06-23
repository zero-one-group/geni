(ns zero-one.geni.utils)

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn vector-of-doubles? [value]
  (and (vector? value) (every? double? value)))

(defmacro with-dynamic-import [imports & body]
  (if (try
        (assert (= 'import (first imports)) "The first form must be an import.")
        (eval imports)
        (catch ClassNotFoundException _ nil)
        (catch AssertionError _ nil))
    `(do ~@body :succeeded)
    :failed))
