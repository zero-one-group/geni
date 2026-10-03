(ns zero-one.geni.function-docs
  "Writes resources/spark-function-docs.edn: the docstrings of the functions
  in `zero-one.geni.core.functions`' table, from the Scaladoc in Spark's
  functions.scala. Run it after adding rows to the table, with the file from
  the Spark tag that `:spark-4` pins, such as
  sql/api/src/main/scala/org/apache/spark/sql/functions.scala at v4.2.0:

    clojure -X:spark:test zero-one.geni.function-docs/write! :scala '\"functions.scala\"'"
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as string]
   [zero-one.geni.core.function-table :as function-table]
   [zero-one.geni.core.functions]
   [zero-one.geni.utils :refer [->kebab-case]]))

(def ^:private def-pattern
  #"(?s)/\*\*((?:(?!\*/).)*?)\*/\s*((?:(?://|@)[^\n]*\n\s*)*)((?:private\[\w+\]\s+|private\s+)?)def (\w+)")

(defn- link-text [[_ target text]]
  (or text (last (string/split target #"[.#]"))))

(defn- clean-text [text]
  (-> text
      (string/replace #"(?s)\{\{\{.*?\}\}\}" "")
      (string/replace #"\[\[([^\]\s]+)(?:\s+([^\]]+))?\]\]" link-text)
      (string/replace #"(?s)<a\s+href=\"[^\"]*\">(.*?)</a>" "$1")
      (string/replace #"</?code>" "`")
      (string/replace #"<li>" "- ")
      (string/replace #"</?(p|ul|ol|li|br|b|i|em)\s*/?>" "")
      (string/replace #"(?m)^(For example|For instance|Example)[^\n]*:\s*$" "")
      (string/replace #"[ \t]+\n" "\n")
      (string/replace #"\n{3,}" "\n\n")
      string/trim))

(defn- parse-doc
  "The description, params and notes of one Scaladoc comment."
  [comment-body]
  (let [lines  (->> (string/split-lines comment-body)
                    (map #(string/replace % #"^\s*\*\s?" "")))
        blocks (reduce (fn [blocks line]
                         (if-let [[_ tag more] (re-matches #"@(\w+)\s*(.*)" line)]
                           (conj blocks {:tag tag :lines [more]})
                           (update-in blocks [(dec (count blocks)) :lines] conj line)))
                       [{:tag "description" :lines []}]
                       lines)]
    (reduce (fn [doc {:keys [tag lines]}]
              (let [text (clean-text (string/join "\n" lines))]
                (case tag
                  "description" (assoc doc :description text)
                  "param"       (let [[_ param more] (re-matches #"(?s)(\w+)\s*(.*)" text)]
                                  (update doc :params (fnil conj [])
                                          [(->kebab-case param) (string/replace more #"\s*\n\s*" " ")]))
                  "note"        (update doc :notes (fnil conj []) text)
                  "return"      (assoc doc :returns (string/replace text #"\s*\n\s*" " "))
                  doc)))
            {}
            blocks)))

(defn scala-docs
  "The parsed docs of the public defs in functions.scala, keyed by name, with
  each overload's in the order the file has them."
  [scala-source]
  (reduce (fn [docs [_ comment-body _ private name]]
            (if (string/blank? private)
              (update docs name (fnil conj []) (parse-doc comment-body))
              docs))
          {}
          (re-seq def-pattern scala-source)))

(defn- docstring
  "One docstring from the overloads' docs: the first description, and the
  params and notes of them all, without repeats."
  [overloads]
  (let [description (or (some #(not-empty (:description %)) overloads)
                        (some #(when-let [returns (:returns %)]
                                 (str "Returns " returns (when-not (re-find #"[.!?]$" returns) ".")))
                              overloads))
        params      (->> (mapcat :params overloads)
                         (reduce (fn [seen [param text]]
                                   (if (some #(= param (first %)) seen) seen (conj seen [param text])))
                                 []))
        notes       (distinct (mapcat :notes overloads))]
    (string/join "\n\n"
                 (remove string/blank?
                         (concat [description
                                  (string/join "\n" (for [[param text] params]
                                                      (str "`" param "`: " text)))]
                                 notes)))))

(defn write!
  "Writes resources/spark-function-docs.edn from functions.scala at `:scala`."
  [{:keys [scala]}]
  (let [docs     (scala-docs (slurp (str scala)))
        names    (sort (map :spark (vals @function-table/table)))
        missing  (remove docs names)
        out      (into (sorted-map)
                       (for [spark-name names
                             :when (docs spark-name)]
                         [spark-name (docstring (docs spark-name))]))]
    (when (seq missing)
      (println "No Scaladoc for:" (string/join ", " missing)))
    (spit "resources/spark-function-docs.edn"
          (with-out-str (pprint/pprint out)))
    (println "Wrote" (count out) "docstrings.")))
