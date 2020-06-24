(ns zero-one.geni.utils)

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn vector-of-doubles? [value]
  (and (vector? value) (every? double? value)))

(defn- import-class
  ([cls] (.importClass *ns* (clojure.lang.RT/classForName (str cls))))
  ([pkg cls] (import-class (str pkg \. cls))))

(defmacro with-dynamic-import [imports & body]
  (if (try
        (doall
          (for [imp imports]
            (if (symbol? imp)
              (import-class imp)
              (let [package (first imp)
                    classes (rest imp)]
                (doall (for [cls classes] (import-class package cls)))))))
        true
        (catch ClassNotFoundException _ nil))
    `(do ~@body :succeeded)
    :failed))
