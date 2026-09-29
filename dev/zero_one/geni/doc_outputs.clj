(ns zero-one.geni.doc-outputs
  "Refreshes the outputs shown in the docs from real runs.

  The Clojure blocks in the README, docs/ and the cookbook run as tests
  through test-doc-blocks, which checks a form's value against a `;; =>`
  comment after it, and what the form prints against a `;; =stdout=>` comment.
  `regen` evaluates each doc's blocks in order, in a fresh namespace, and
  rewrites those comments with what the forms return and print. It leaves the
  rest of the doc alone, and skips the blocks that test-doc-blocks skips.
  See CONTRIBUTING.md."
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :as pprint]
   [clojure.string :as string])
  (:import
   (clojure.lang LineNumberingPushbackReader)
   (java.io StringReader StringWriter)))

(def ^:private fence-open #"^(\s*)```(?i:clojure|clj)\s*$")
(def ^:private fence-close #"^\s*```\s*$")
(def ^:private options-comment #"^\s*<!--\s+(.+:test-doc-blocks/skip.+|:test-doc-blocks/skip)\s+-->\s*$")
(def ^:private expectation #"^(\s*)(?:;;\s*)?(=>|=stdout=>)\s?(.*)$")
(def ^:private continuation #"^\s*;+ ?(.*)$")

(defn- parseable? [s]
  (try
    (binding [*read-eval* false]
      (read-string {:read-cond :allow} s))
    true
    (catch Exception _ false)))

(defn- take-value
  "The lines of a `;; =>` expectation: as many as it takes to read a form,
  as test-doc-blocks does."
  [payload lines]
  (loop [text [payload] n 0]
    (if (or (parseable? (string/join "\n" text)) (= n (count lines)))
      n
      (let [line (nth lines n)]
        (recur (conj text (or (second (re-matches continuation line)) line)) (inc n))))))

(defn- take-stdout
  "The lines of a `;; =stdout=>` expectation: the comment lines that follow."
  [lines]
  (count (take-while #(and (re-matches continuation %) (not (re-matches expectation %)))
                     lines)))

(defn- segments
  "Splits a block's lines into code and expectations, with each expectation's
  position in the block."
  [lines]
  (loop [i 0 code [] segs []]
    (if (= i (count lines))
      (cond-> segs (seq code) (conj {:code code}))
      (let [line (nth lines i)]
        (if-let [[_ indent kind payload] (re-matches expectation line)]
          (let [more (subvec lines (inc i))
                n    (if (= kind "=>") (take-value payload more) (take-stdout more))]
            (recur (+ i 1 n)
                   []
                   (cond-> segs
                     (seq code) (conj {:code code})
                     true       (conj {:kind   (if (= kind "=>") :value :stdout)
                                       :indent indent
                                       :start  i
                                       :end    (+ i 1 n)}))))
          (recur (inc i) (conj code line) segs))))))

(defn- read-forms [ns text]
  (let [rdr (LineNumberingPushbackReader. (StringReader. text))]
    (binding [*ns* ns *read-eval* false]
      (loop [forms []]
        (let [form (read {:eof ::eof :read-cond :allow} rdr)]
          (if (= ::eof form) forms (recur (conj forms form))))))))

(defn- run-form
  "Evaluates a form, and returns its value and what it printed."
  [ns form]
  (let [out (StringWriter.)]
    (binding [*ns* ns *out* out]
      (let [value (eval form)]
        (when (seq? value) (dorun value))
        {:value value :out (str out)}))))

(defn- render-value
  "A value as a `;; =>` comment: on one line if it fits, or pretty-printed.
  A value that doesn't read back as itself, such as a keyword with a space,
  becomes a `; =>` comment, which test-doc-blocks doesn't check."
  [indent value]
  (let [text (binding [pprint/*print-right-margin* 80
                       *print-length*              nil
                       *print-level*               nil]
               (let [one-line (pr-str value)]
                 (if (<= (count one-line) 90)
                   one-line
                   (string/trimr (with-out-str (pprint/pprint value))))))
        [line & more] (string/split-lines text)
        checked? (and (parseable? text)
                      (binding [*read-eval* false] (= value (read-string text))))
        prefix   (if checked? ";;" ";")]
    (when-not checked?
      (println "  note: a value that doesn't read back as itself, left unchecked:"
               (subs line 0 (min 60 (count line)))))
    (into [(str indent prefix " => " line)]
          (map #(str indent prefix "    " %) more))))

(defn- render-stdout
  "Printed output as a `;; =stdout=>` comment. Lines that end in spaces, as
  `g/show-vertical`'s do, can't be checked, since test-doc-blocks trims the
  expected lines but not the output, so they become a plain comment."
  [indent out]
  (let [lines    (string/split-lines out)
        checked? (not-any? #(re-find #"\s$" %) lines)]
    (when-not checked?
      (println "  note: output with lines that end in spaces, left unchecked"))
    (into (if checked? [(str indent ";; =stdout=>")] [])
          (map #(if (string/blank? %) (str indent ";") (str indent "; " (string/trimr %))) lines))))

(defn- regen-block
  "Evaluates a block's code, and returns replacements for its expectations as
  [start end new-lines], relative to the block."
  [ns lines]
  (loop [[seg & more] (segments lines) last-run nil replacements []]
    (cond
      (nil? seg)
      replacements

      (:code seg)
      (let [runs (mapv (fn [form]
                         (try
                           (run-form ns form)
                           (catch Throwable e
                             (println "  failed:" (pr-str form))
                             (throw e))))
                       (read-forms ns (string/join "\n" (:code seg))))]
        (recur more (or (peek runs) last-run) replacements))

      (nil? last-run)
      (throw (ex-info "An expectation with no form before it" seg))

      :else
      (let [{:keys [kind indent start end]} seg
            {:keys [value out]} last-run
            new-lines (case kind
                        :value  (render-value indent value)
                        :stdout (render-stdout indent out))]
        (recur more last-run (conj replacements [start end new-lines]))))))

(defn- blocks
  "The Clojure blocks that test-doc-blocks turns into tests, as [first last]
  line indices of their contents."
  [lines]
  (loop [i 0 skip? false skip-all? false found []]
    (if (>= i (count lines))
      found
      (let [line (nth lines i)]
        (cond
          (re-matches options-comment line)
          (let [opts (edn/read-string (second (re-matches options-comment line)))
                opts (if (keyword? opts) {opts true} opts)
                skip? (:test-doc-blocks/skip opts)
                skip-all? (if (= :all-next (:test-doc-blocks/apply opts)) skip? skip-all?)]
            (recur (inc i) skip? skip-all? found))

          (re-matches fence-open line)
          (let [close (->> (range (inc i) (count lines))
                           (filter #(re-matches fence-close (nth lines %)))
                           first)]
            (when-not close
              (throw (ex-info "Unclosed Clojure fence" {:line (inc i)})))
            (recur (inc close) skip-all? skip-all? (cond-> found (not skip?) (conj [(inc i) close]))))

          (string/blank? line)
          (recur (inc i) skip? skip-all? found)

          :else
          (recur (inc i) skip-all? skip-all? found))))))

(defn regen-file
  "Rewrites the expectations in one doc, and returns whether it changed."
  [path]
  (println "Running" path)
  (let [text   (slurp path)
        lines  (vec (string/split-lines text))
        ns-sym (symbol (str "doc-outputs." (string/replace path #"[^A-Za-z0-9]+" "-")))
        _      (remove-ns ns-sym)
        ns     (create-ns ns-sym)
        _      (binding [*ns* ns] (refer-clojure))
        edits  (try
                 (vec
                  (for [[from to] (blocks lines)
                        [start end new-lines] (try
                                                (regen-block ns (subvec lines from to))
                                                (catch Throwable e
                                                  (throw (ex-info (str path ", block at line " from ": " (ex-message e))
                                                                  {:path path :line from} e))))]
                    [(+ from start) (+ from end) new-lines]))
                 (finally (remove-ns ns-sym)))
        result (loop [out [] i 0 [[start end new-lines] & more :as edits] edits]
                 (cond
                   (= i (count lines)) out
                   (and start (= i start)) (recur (into out new-lines) end more)
                   :else (recur (conj out (nth lines i)) (inc i) edits)))
        new-text (str (string/join "\n" result) (when (string/ends-with? text "\n") "\n"))]
    (when (not= text new-text)
      (spit path new-text)
      (println "  updated" path))
    (not= text new-text)))

(defn regen
  "Refreshes the outputs in the given docs from real runs, e.g.
  `clojure -X:spark:test:doc-tests zero-one.geni.doc-outputs/regen :files '[\"README.md\"]'`.
  Review the diff before committing it."
  [{:keys [files]}]
  (doseq [path files]
    (regen-file path))
  (shutdown-agents)
  (System/exit 0))
