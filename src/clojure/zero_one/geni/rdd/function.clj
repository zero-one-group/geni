;; Taken from https://github.com/amperity/sparkplug
(ns zero-one.geni.rdd.function
  (:require
   [clojure.string :as str])
  (:import
   (java.lang.reflect Field)
   (java.util Collections HashSet IdentityHashMap Set)))

(defn access-field [^Field field obj]
  (try
    (.setAccessible field true)
    (.get field obj)
    (catch Exception _ nil))) ;; Original was IllegalAccessException

(defn- lazy-seq?
  "Whether `obj` is a seq that may be lazy or infinite, such as `(range)`,
  which walking, or even hashing, would realise."
  [obj]
  (and (seq? obj) (not (list? obj))))

(defn- collection? [obj]
  (or (coll? obj)
      (instance? java.util.Collection obj)
      (instance? java.util.Map obj)))

(defn- type-namespace
  "The namespace that defines the record or type called `class-sym`, if it's
  loaded."
  [class-sym]
  (let [ns-sym (symbol (str/replace (str class-sym) #"\.[^.]+$" ""))]
    (when (find-ns ns-sym)
      ns-sym)))

(defn- fn-namespace
  "The namespace where the function `f` was defined. A function defined in a
  record's or type's method is a class inside the record's class, so it's the
  namespace that defines the record. A keyword or a set used as a function
  names a class of clojure.lang, and has none. A loaded namespace wins over a
  class of the same name, such as the one that `:gen-class` makes."
  [f]
  (let [enclosing (-> (.getName (class f))
                      (Compiler/demunge)
                      (str/split #"/")
                      (first)
                      (symbol))]
    (cond
      (find-ns enclosing)                                           enclosing
      (class? (resolve (symbol (str/replace (str enclosing) "-" "_")))) (type-namespace enclosing)
      :else                                                         enclosing)))

(defn walk-object-vars
  "Adds to `references` the namespaces of the vars, and of the functions, that
  `obj` holds, as its fields or as the elements of a collection, however
  deeply. `visited` keeps it from walking an object twice."
  [^Set references ^Set visited obj]
  (when-not (or (nil? obj)
                (boolean? obj)
                (string? obj)
                (number? obj)
                (keyword? obj)
                (symbol? obj)
                (instance? clojure.lang.Ref obj)
                (lazy-seq? obj)
                (.contains visited obj))
    (.add visited obj)
    (cond
      (var? obj)
      (.add references (ns-name (:ns (meta obj))))

      ;; A function's static fields hold the vars it uses, and its other
      ;; fields the values it closes over.
      (fn? obj)
      (do
        (when-let [ns-sym (fn-namespace obj)]
          (.add references ns-sym))
        (doseq [^Field field (.getDeclaredFields (class obj))]
          (walk-object-vars references visited (access-field field obj))))

      ;; Vectors, maps, sets, records and Java collections.
      (collection? obj)
      (doseq [entry obj]
        (walk-object-vars references visited entry))

      ;; Other objects: only the Clojure functions and collections they hold,
      ;; rather than all of an object graph such as a SparkContext's.
      :else
      (doseq [^Field field (.getDeclaredFields (class obj))]
        (let [value (access-field field obj)]
          (when (or (ifn? value) (coll? value))
            (walk-object-vars references visited value)))))))

(defn namespace-references
  "The namespaces that the executors need to load to run `obj`, a function:
  the one that defines it, and those of the vars and functions that it uses
  or closes over, except clojure.core."
  [^Object obj]
  (let [references (HashSet.)
        ;; By identity, so that walking never hashes a large collection.
        visited    (Collections/newSetFromMap (IdentityHashMap.))]
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
