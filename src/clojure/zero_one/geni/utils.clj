(ns zero-one.geni.utils
  (:require
   [clojure.string :as string]))

(defn coalesce [& xs]
  (first (filter (complement nil?) xs)))

(defn ensure-coll [x] (if (or (coll? x) (nil? x)) x [x]))

(defn class-named
  "The class with this name, or nil when it isn't on the classpath. Some
  classes are only there with classic Spark, such as MLlib's, which a Spark
  Connect client doesn't bring."
  ^Class [^String class-name]
  (try
    (Class/forName class-name false (clojure.lang.RT/baseLoader))
    (catch ClassNotFoundException _ nil)
    (catch LinkageError _ nil)))

(defn optional-fn
  "The function that `sym` names, in a namespace of an optional dependency,
  loaded when it's first needed, or an error with `message`, which says what
  to add."
  [sym message]
  (or (try (requiring-resolve sym) (catch Exception _ nil))
      (throw (ex-info message {:fn sym}))))

(defn arg-count [f]
  (let [m (first (.getDeclaredMethods (class f)))
        p (.getParameterTypes m)]
    (alength p)))

(defn ->string-map
  "Spark's options as strings: keywords and symbols by name, and other values,
  such as `true`, through `str`."
  [m]
  (into {} (map (fn [[k v]] [(name k) (if (ident? v) (name v) (str v))])) m))

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

(defn import-var
  "Interns `alias` in the current namespace as a copy of the var `src`, with
  its docstring and arglists, a macro when `src` is, and following `src`.
  Returns the new var."
  [^clojure.lang.Var src alias]
  (let [dst (intern *ns* alias @src)]
    (alter-meta! dst merge (dissoc (meta src) :name :ns))
    (when (.isMacro src)
      (.setMacro dst))
    (link-vars src dst)
    dst))

(defmacro import-fn
  "Defines `alias` in the current namespace as a copy of the var `sym`,
  keeping its docstring and arglists. It's one call, so that a namespace
  that imports hundreds of vars, as `zero-one.geni.core` does, still fits the
  JVM's limit on a method's size when it's compiled ahead of time."
  [sym alias]
  (let [^clojure.lang.Var src (resolve sym)]
    (when-not (var? src)
      (throw (IllegalArgumentException. (str "Can't import " sym))))
    `(import-var (var ~(symbol (str (.-ns src)) (str (.-sym src)))) '~alias)))

(defmacro import-vars
  "Imports vars under their own names: (import-vars [ns a b] [other-ns c])."
  [& specs]
  `(do
     ~@(for [[ns-sym & syms] specs
             sym             syms]
         `(import-fn ~(symbol (str ns-sym) (str sym)) ~sym))))
