(ns zero-one.geni.kondo-hooks
  "Tells clj-kondo what `zero-one.geni.utils/import-fn` defines.")

(defmacro import-fn [sym alias]
  `(def ~alias ~sym))
