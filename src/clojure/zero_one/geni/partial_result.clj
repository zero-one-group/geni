(ns zero-one.geni.partial-result)

(defn final-value [result] (.getFinalValue result))

(defn initial-value [result] (.initialValue result))

(defn is-initial-value-final [result] (.isInitialValueFinal result))
(def final? is-initial-value-final)

