(ns zero-one.geni.kondo-hooks
  "Tells clj-kondo what `zero-one.geni.utils/import-fn`,
  `zero-one.geni.core.function-table/def-spark-functions` and
  `zero-one.geni.interop/def-stages` define.")

(defmacro import-fn [sym alias]
  `(def ~alias ~sym))

(defmacro def-spark-functions [& rows]
  `(do ~@(for [[fn-name & more] rows]
           `(defn ~fn-name
              ~@(for [arglist (take-while vector? more)]
                  `(~arglist ~(vec (remove #{'&} arglist))))))))

(defmacro def-stages [_package & rows]
  `(do ~@(for [[fn-name] rows]
           `(defn ~fn-name [~'params] ~'params))))
