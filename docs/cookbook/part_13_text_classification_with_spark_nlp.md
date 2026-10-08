# CB-13: Text Classification with Spark NLP

[Spark NLP](https://github.com/JohnSnowLabs/spark-nlp), from John Snow Labs, is a natural language processing library for Spark. Its annotators are Spark ML stages, so they go into `ml/pipeline` beside Spark's own stages, and `ml/stage` builds them from a map of params, as Geni's own stage functions take one. This part classifies text messages as spam or not: Spark NLP cleans the words and reduces them to their dictionary forms, and Spark's TF-IDF and logistic regression do the rest, in one pipeline.

## 13.1 Adding Spark NLP

Spark NLP isn't one of Geni's dependencies. With the README's `:spark` setup, Spark 3.5 on Scala 2.12, add its Scala 2.12 build:

```edn
{:deps {com.johnsnowlabs.nlp/spark-nlp_2.12 {:mvn/version "7.0.0"}}}
```

For Spark 4, Spark NLP publishes `spark-nlp_2.13`, which it supports on Spark 4.0.1 to 4.1, and `spark-nlp-spark400_2.13` for Spark 4.0.0 as released. Its deep-learning annotators need its `spark-nlp-silicon` build on Apple Silicon and `spark-nlp-aarch64` on Linux on ARM, but the annotators in this part run on the plain build. Spark NLP's [installation guide](https://sparknlp.org/docs/en/install) has the details. As with the rest of `ml`, it needs classic Spark rather than Spark Connect.

Some annotators are pretrained models. Spark NLP downloads one from its public repository the first time it's used, into `~/cache_pretrained`, and loads it from there afterwards. The `spark.jsl.settings.pretrained.cache_folder` config puts them elsewhere.

## 13.2 The SMS Spam Collection

The [SMS Spam Collection](https://archive.ics.uci.edu/dataset/228/sms+spam+collection), by Tiago A. Almeida and José María Gómez Hidalgo, has 5,574 text messages, each labelled spam or ham. A copy is on GitHub as a tab-separated file without a header. Some messages contain double quotes, so `:quote ""` turns quoting off:

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(defn download-data! [source-url target-path]
  (if (.exists (io/file target-path))
    :already-exists
    (do
      (io/make-parents target-path)
      (with-open [in (io/input-stream source-url)]
        (io/copy in (io/file target-path)))
      :downloaded)))

(download-data!
  "https://raw.githubusercontent.com/justmarkham/pycon-2016-tutorial/61c51b26e73ecfc6aa774aea3673c3b122804343/data/sms.tsv"
  "data/cookbook/sms.tsv")

(def messages
  (-> (g/read-csv! "data/cookbook/sms.tsv" {:sep "\t" :header false :quote ""})
      (g/to-df :label :text)
      (g/with-column :spam (g/double (g/= :label (g/lit "spam"))))))

(g/count messages)
;; => 5574

(-> messages
    (g/group-by :label)
    (g/agg {:n (g/count "*")})
    (g/order-by :label)
    g/show)
;; =stdout=>
; +-----+----+
; |label|n   |
; +-----+----+
; |ham  |4827|
; |spam |747 |
; +-----+----+
```

## 13.3 Annotators as Stages

Spark NLP's annotators read and write annotation columns: arrays of structs, each with the annotation's type, where it is in the text, its result and its metadata. A `DocumentAssembler` turns a text column into the `document` annotation that the others start from. Then a `Tokenizer` splits it into tokens, a `Normalizer` lower-cases them and drops what isn't a letter, and a `StopWordsCleaner` drops common words such as "the".

`ml/stage` makes each one from its class, with its params in kebab case, as Geni's own stages take them: `:input-cols` goes to `setInputCols`. In plain interop, the tokeniser would be `(doto (Tokenizer.) (.setInputCols (into-array String ["document"])) (.setOutputCol "token"))`. `ml/stage` also takes a class's name as a string, such as `"com.johnsnowlabs.nlp.DocumentAssembler"`, in place of the import.

```clojure
(import '(com.johnsnowlabs.nlp DocumentAssembler Finisher)
        '(com.johnsnowlabs.nlp.annotators LemmatizerModel Normalizer StopWordsCleaner Tokenizer))

(def document-assembler
  (ml/stage DocumentAssembler {:input-col :text :output-col :document}))

(def tokenizer
  (ml/stage Tokenizer {:input-cols [:document] :output-col :token}))

(def normalizer
  (ml/stage Normalizer {:input-cols [:token] :output-col :normalized :lowercase true}))

(def stop-words-cleaner
  (ml/stage StopWordsCleaner {:input-cols [:normalized] :output-col :cleaned}))
```

The lemmatiser turns each word into its dictionary form, such as "tickets" into "ticket". It's a pretrained model: `LemmatizerModel/pretrained` gives `lemma_antbnc`, an English one, which is downloaded the first time. Given a stage that exists already, `ml/stage` sets its params in place:

```clojure
(def lemmatizer
  (ml/stage (LemmatizerModel/pretrained "lemma_antbnc" "en")
            {:input-cols [:cleaned] :output-col :lemma}))
```

The tokeniser and the normaliser are estimators, as Spark's `StringIndexer` is, so a pipeline of these stages is fitted before it transforms. Each annotation is a struct, which `g/collect` gives as a map:

```clojure
(def text-pipeline
  (ml/pipeline document-assembler tokenizer normalizer stop-words-cleaner lemmatizer))

(def annotated
  (ml/transform messages (ml/fit messages text-pipeline)))

(-> annotated (g/select :lemma) (g/limit 1) g/collect first :lemma first)
;; => {:annotatorType "token",
;;     :begin 0,
;;     :end 1,
;;     :result "go",
;;     :metadata {"sentence" "0"},
;;     :embeddings ()}
```

A `Finisher` turns annotations back into plain columns, here an array of strings per message:

```clojure
(def finisher
  (ml/stage Finisher {:input-cols [:lemma] :output-cols [:lemmas]}))

(-> annotated
    (ml/transform finisher)
    (g/select :lemmas)
    (g/limit 3)
    g/collect)
;; => ({:lemmas
;;      ("go"
;;       "jurong"
;;       "point"
;;       "crazy"
;;       "available"
;;       "bugis"
;;       "n"
;;       "great"
;;       "world"
;;       "la"
;;       "e"
;;       "buffet"
;;       "cine"
;;       "get"
;;       "amore"
;;       "wat")}
;;     {:lemmas ("ok" "lar" "joke" "wif" "u" "oni")}
;;     {:lemmas
;;      ("free"
;;       "entry"
;;       "wkly"
;;       "comp"
;;       "win"
;;       "fa"
;;       "cup"
;;       "final"
;;       "tkts"
;;       "st"
;;       "may"
;;       "text"
;;       "fa"
;;       "receive"
;;       "entry"
;;       "questionstd"
;;       "txt"
;;       "ratetcs"
;;       "apply"
;;       "over")})
```

## 13.4 One Pipeline

The Finisher's arrays of words are what Spark's own `CountVectorizer` takes, so Spark NLP's stages and Spark's go into one pipeline. `CountVectorizer` keeps the words that are in at least two messages, `IDF` weighs them by how rare they are, and a logistic regression learns to tell spam from ham:

```clojure
(def spam-pipeline
  (ml/pipeline
   document-assembler
   tokenizer
   normalizer
   stop-words-cleaner
   lemmatizer
   finisher
   (ml/count-vectorizer {:input-col :lemmas :output-col :counts :min-df 2.0})
   (ml/idf {:input-col :counts :output-col :features})
   (ml/logistic-regression {:label-col :spam :max-iter 50})))

(def splits (g/random-split messages [0.8 0.2] 1234))
(def training-data (first splits))
(def test-data (second splits))

(def spam-model (ml/fit training-data spam-pipeline))

(def predictions (ml/transform test-data spam-model))
```

The area under the ROC curve on the messages held out, and how many of them land in each class:

```clojure
(ml/evaluate predictions (ml/binary-classification-evaluator {:label-col :spam}))
;; => 0.9480284334763985

(-> predictions
    (g/group-by :spam :prediction)
    (g/agg {:n (g/count "*")})
    (g/order-by :spam :prediction)
    g/show)
;; =stdout=>
; +----+----------+---+
; |spam|prediction|n  |
; +----+----------+---+
; |0.0 |0.0       |887|
; |0.0 |1.0       |45 |
; |1.0 |0.0       |14 |
; |1.0 |1.0       |130|
; +----+----------+---+
```

The fitted pipeline takes new text as it is, since every stage, Spark NLP's included, is part of the model:

```clojure
(-> (g/table->dataset [["Congratulations, you have won a free ticket. Text WIN to 80086 now"]
                       ["Are we still meeting for lunch tomorrow?"]]
                      [:text])
    (ml/transform spam-model)
    (g/select :text :prediction)
    g/show)
;; =stdout=>
; +------------------------------------------------------------------+----------+
; |text                                                              |prediction|
; +------------------------------------------------------------------+----------+
; |Congratulations, you have won a free ticket. Text WIN to 80086 now|1.0       |
; |Are we still meeting for lunch tomorrow?                          |0.0       |
; +------------------------------------------------------------------+----------+
```

Spark NLP has many more annotators, and pretrained models for named entities, sentiment and embeddings, among others. Its [models hub](https://sparknlp.org/models) lists them, and each goes into a pipeline the same way.
