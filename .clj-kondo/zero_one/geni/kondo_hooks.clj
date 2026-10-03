(ns zero-one.geni.kondo-hooks
  "Tells clj-kondo what `zero-one.geni.utils/import-fn` and
  `zero-one.geni.core.function-table/def-spark-functions` define.")

(defmacro import-fn [sym alias]
  `(def ~alias ~sym))

(defmacro def-spark-functions [& rows]
  `(do ~@(for [[fn-name & more] rows]
           `(defn ~fn-name
              ~@(for [arglist (take-while vector? more)]
                  `(~arglist ~(vec (remove #{'&} arglist))))))))
