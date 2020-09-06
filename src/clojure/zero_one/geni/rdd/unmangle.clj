;; Taken from https://github.com/amperity/sparkplug
(ns zero-one.geni.rdd.unmangle
  (:require
    [clojure.string :as string])
  (:import
    (clojure.lang Compiler)))

(defn- internal-call?  [^StackTraceElement element]
  (let [class-name (.getClassName element)]
    (or (string/starts-with? class-name "zero_one.geni")
        (string/starts-with? class-name "clojure.lang."))))

(defn- stack-callsite []
  (first (remove internal-call? (.getStackTrace (Exception.)))))

(defn- callsite-name []
  (let [callsite (stack-callsite)
        filename (.getFileName callsite)
        classname (.getClassName callsite)
        line-number (.getLineNumber callsite)]
    (format "%s %s:%d" (Compiler/demunge classname) filename line-number)))

(defn set-callsite-name [rdd & args]
  (let [rdd-name (format "#<%s: %s %s>"
                         (.getSimpleName (class rdd))
                         (callsite-name)
                         (if (seq args)
                           (str " [" (string/join ", " args) "]")
                           ""))]
    (.setName rdd rdd-name)))

(defn ->fn-name [maybe-f]
  (if (fn? maybe-f)
    (Compiler/demunge (.getName (class maybe-f)))
    maybe-f))

(defn unmangle-name [rdd & args]
  (apply set-callsite-name rdd (map ->fn-name args)))
