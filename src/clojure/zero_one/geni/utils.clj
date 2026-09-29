(ns zero-one.geni.utils
  (:require
   [clojure.string :as string]))

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn- import-class
  ([cls] (.importClass *ns* (clojure.lang.RT/classForName (str cls))))
  ([pkg cls] (import-class (str pkg \. cls))))

(defmacro with-dynamic-import [imports & body]
  (if (try
        (doall
         (for [imp imports]
           (if (symbol? imp)
             (import-class imp)
             (let [[pkg & classes] imp]
               (doall (for [cls classes] (import-class pkg cls)))))))
        true
        (catch ClassNotFoundException _ nil))
    `(do ~@body :succeeded)
    :failed))

(defn arg-count [f]
  (let [m (first (.getDeclaredMethods (class f)))
        p (.getParameterTypes m)]
    (alength p)))

(defn ->string-map [m]
  (->> m
       (map (fn [[k v]] [(name k) (name v)]))
       (into {})))

;; Case conversions. These split words the same way camel-snake-kebab does,
;; so column and param names come out as they did before.
(defn- char-kind [c]
  (cond
    (<= (int \0) (int c) (int \9))                           :number
    (#{\- \_ \space \tab \newline \u000b \formfeed \return} c) :whitespace
    (<= (int \a) (int c) (int \z))                           :lower
    (<= (int \A) (int c) (int \Z))                           :upper
    :else                                                   :other))

(defn- split-words
  "Splits on -, _ and whitespace, and between words in camelCase, PascalCase
  and ACRONYMWords, and before digits."
  [^String s]
  (let [kinds (mapv char-kind s)
        n     (count kinds)]
    (loop [words [] start 0 i 0]
      (let [with-word (fn [end]
                        (if (> end start) (conj words (subs s start end)) words))]
        (cond
          (>= i n)                        (or (seq (with-word i)) [""])
          (= (kinds i) :whitespace)       (recur (with-word i) (inc i) (inc i))
          (let [a (kinds i)
                b (get kinds (inc i))
                c (get kinds (+ i 2))]
            (or (and (not= a :upper) (= b :upper))
                (and (not= a :number) (= b :number))
                (and (= a :upper) (= b :upper) (= c :lower))))
          (recur (with-word (inc i)) (inc i) (inc i))
          :else                           (recur words start (inc i)))))))

(defn ->kebab-case
  "\"MaxIter\" -> \"max-iter\""
  [s]
  (string/join "-" (map string/lower-case (split-words (name s)))))

(defn ->camel-case
  "\"infer-schema\" -> \"inferSchema\""
  [s]
  (let [[word & words] (split-words (name s))]
    (apply str (string/lower-case word) (map string/capitalize words))))

;; Importing vars from other namespaces, so that `zero-one.geni.core` can
;; offer one flat API.
(defn link-vars
  "Makes `dst` follow `src`, so re-evaluating `src` at the REPL also updates
  the imported var."
  [src dst]
  (add-watch src [::link dst]
             (fn [_ _ _ value]
               (alter-var-root dst (constantly value)))))

(defmacro import-fn
  "Defines `alias` in the current namespace as a copy of the var `sym`,
  keeping its docstring and arglists."
  [sym alias]
  (let [^clojure.lang.Var src (resolve sym)]
    (when-not (var? src)
      (throw (IllegalArgumentException. (str "Can't import " sym))))
    (let [src-sym (symbol (str (.-ns src)) (str (.-sym src)))]
      `(do
         (def ~alias @(var ~src-sym))
         (alter-meta! (var ~alias) merge (dissoc (meta (var ~src-sym)) :name :ns))
         ~@(when (:macro (meta src)) [`(.setMacro (var ~alias))])
         (link-vars (var ~src-sym) (var ~alias))
         (var ~alias)))))

(defmacro import-vars
  "Imports vars under their own names: (import-vars [ns a b] [other-ns c])."
  [& specs]
  `(do
     ~@(for [[ns-sym & syms] specs
             sym             syms]
         `(import-fn ~(symbol (str ns-sym) (str sym)) ~sym))))
