(ns zero-one.geni.interop
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.java.data :as j]
    [clojure.string :refer [replace-first]]))

(defn setter? [^java.lang.reflect.Method method]
  (and (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (re-find #"^set[A-Z]" (.getName method))))

(defn setter-keyword [^java.lang.reflect.Method method]
  (-> method
      .getName
      (replace-first #"set" "")
      ->kebab-case
      keyword))

(defn setters-map [^Class cls]
  (->> cls
       .getMethods
       (filter setter?)
       (map #(vector (setter-keyword %) %))
       (into {})))

(defn setter-type [^java.lang.reflect.Method method]
  (get (.getParameterTypes method) 0))

(defn set-value [^java.lang.reflect.Method method instance value]
  (.invoke method instance (into-array [(j/to-java (setter-type method) value)])))
