;(ns zero-one.geni.rdd.function)

;(defmacro gen-function [cls wrapper-name]
  ;`(defn ~wrapper-name [f#]
     ;(new ~(symbol (str "zero_one.geni.rdd.function." cls)) f#)))

;(gen-function Function function)
;(gen-function Function2 function2)
;(gen-function VoidFunction void-function)
;(gen-function FlatMapFunction flat-map-function)
;(gen-function FlatMapFunction2 flat-map-function2)
;(gen-function PairFlatMapFunction pair-flat-map-function)
;(gen-function PairFunction pair-function)

;; Taken from sparkplug:
;; https://github.com/amperity/sparkplug/blob/master/sparkplug-core/src/clojure/sparkplug/function.clj

(ns zero-one.geni.rdd.function
  (:require
    [clojure.string :as str])
  (:import
    (java.lang.reflect Field Modifier)
    (java.util HashSet)))

(defn- access-field [^Field field obj]
  (let [accessible? (.isAccessible field)]
    (try
      (when-not accessible?
        (.setAccessible field true))
      (.get field obj)
      (catch IllegalAccessException _ nil))))

(defn- walk-object-vars [^HashSet references ^HashSet visited obj]
  (when-not (or (nil? obj)
                (boolean? obj)
                (string? obj)
                (number? obj)
                (keyword? obj)
                (symbol? obj)
                (instance? clojure.lang.Ref obj)
                (.contains visited obj))
    (.add visited obj)
    (if (var? obj)
      (let [ns-sym (ns-name (:ns (meta obj)))]
        (.add references ns-sym))
      (do
        (when (map? obj)
          (doseq [entry obj]
            (walk-object-vars references visited entry)))
        (doseq [^Field field (.getDeclaredFields (class obj))]
          (when (or (not (map? obj)) (Modifier/isStatic (.getModifiers field)))
            (let [value (access-field field obj)]
              (when (or (ifn? value) (map? value))
                (walk-object-vars references visited value)))))))))

(defn namespace-references [^Object obj]
  (let [obj-ns (-> (.. obj getClass getName)
                   (Compiler/demunge)
                   (str/split #"/")
                   (first)
                   (symbol))
        references (HashSet.)
        visited (HashSet.)]
    (when-not (class? (resolve obj-ns))
      (.add references obj-ns))
    (walk-object-vars references visited obj)
    (disj (set references) 'clojure.core)))

(defmacro ^:private gen-function
  [fn-name constructor]
  (let [class-sym (symbol (str "zero_one.geni.rdd.function." fn-name))]
    `(defn ~(vary-meta constructor assoc :tag class-sym)
       ~(str "Construct a new serializable " fn-name " function wrapping `f`.")
       [~'f]
       (let [references# (namespace-references ~'f)]
         (new ~class-sym ~'f (mapv str references#))))))

(gen-function Fn1 function)
(gen-function Fn2 function2)
(gen-function FlatMapFn1 flat-map-function)
(gen-function FlatMapFn2 flat-map-function2)
(gen-function PairFlatMapFn pair-flat-map-function)
(gen-function PairFn pair-function)
(gen-function VoidFn void-function)
