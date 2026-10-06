# Clojure UDFs

Spark's built-in functions cover most column work, and Spark's optimiser knows what they do. When none of them does the job, `g/udf` turns a Clojure function into a Spark UDF (user-defined function), which Spark calls for each row, on the executors.

The examples use a small dataset:

```clojure
(require '[clojure.string :as string])
(require '[zero-one.geni.core :as g])

(def dataframe
  (g/records->dataset
   [{:name "Ada" :score 91 :tags ["maths" "engines"]}
    {:name "Grace" :score 78 :tags ["compilers"]}
    {:name "Linus" :score nil :tags []}]))
```

## Calling a UDF

`g/udf` takes the function and the type of what it returns, and gives back a function of columns:

```clojure
(defn grade-of [score]
  (cond
    (nil? score)  "absent"
    (<= 90 score) "A"
    (<= 75 score) "B"
    :else         "C"))

(def grade (g/udf grade-of :string))

(-> dataframe
    (g/select :name {:grade (grade :score)})
    g/collect)
;; => ({:name "Ada", :grade "A"} {:name "Grace", :grade "B"} {:name "Linus", :grade "absent"})
```

The function gets each row's values as Clojure data, as `g/collect` returns them: `nil` for a null, a seq for an array, and a map for a map or a struct. A UDF takes up to ten columns, and calls the function with one argument per column:

```clojure
(def describe
  (g/udf (fn [name score] (str name ": " (grade-of score))) :string))

(-> dataframe
    (g/select {:described (describe :name :score)})
    g/collect)
;; => ({:described "Ada: A"} {:described "Grace: B"} {:described "Linus: absent"})
```

## Return Types

The return type is a type keyword such as `:long`, a schema in the form that `g/->schema` takes, or a Spark `DataType`. The result is converted to it, so a Clojure long becomes an int for `:int`, and a keyword becomes a string for `:string`. A vector of one type is an array, and a map is a struct:

```clojure
(def tag-stats
  (g/udf (fn [tags]
           {:n       (count tags)
            :longest (apply max-key count "" tags)})
         {:n :int :longest :string}))

(def shout
  (g/udf #(map string/upper-case %) [:string]))

(-> dataframe
    (g/select :name {:stats (tag-stats :tags) :loud (shout :tags)})
    g/collect)
;; => ({:name "Ada", :stats {:n 2, :longest "engines"}, :loud ("MATHS" "ENGINES")}
;;     {:name "Grace", :stats {:n 1, :longest "compilers"}, :loud ("COMPILERS")}
;;     {:name "Linus", :stats {:n 0, :longest ""}, :loud ()})
```

A vector of two types, such as `[:string :long]`, describes a map column, with keys of the first type and values of the second. A struct can also come from the values in order, as in `[n longest]`.

## Options

A third argument gives the options. `:name` names the UDF in the column's name and in `g/explain`, which otherwise show `UDF`:

```clojure
(-> dataframe
    (g/select (grade :score) ((g/udf grade-of :string {:name "grade"}) :score))
    g/column-names)
;; => ("UDF(score)" "grade(score)")
```

`:deterministic false` marks a function that can return different results for the same values, as one that draws random numbers can, so that Spark's optimiser doesn't move it, or merge it with other expressions, as it can a deterministic one. It doesn't set how many times Spark calls the function for a row: a task that's retried, or a DataFrame that's computed twice, calls it again, so any side effects have to cope with repeated calls. `:nullable false` tells Spark that the function never returns `nil`.

## SQL

`g/register-udf!` registers the function under a name on the session, for SQL and `g/expr`, and returns the same function of columns as `g/udf`:

```clojure
(g/register-udf! "grade" grade-of :string)

(g/create-or-replace-temp-view! dataframe "scores")

(-> (g/create-spark-session {})
    (g/sql "SELECT name, grade(score) AS grade FROM scores ORDER BY name")
    g/collect)
;; => ({:name "Ada", :grade "A"} {:name "Grace", :grade "B"} {:name "Linus", :grade "absent"})
```

SQL calls a UDF with a fixed number of columns, which is the function's own when it has one arity. For a function with several, such as `+`, `:arity` gives it: `(g/register-udf! "plus" + :long {:arity 2})`.

## Where UDFs Run

- **Locally.** Functions defined at a REPL or in a script work on a local session that Geni starts, as in these examples.
- **On a cluster.** The executors need the function. A var, such as `#'grade-of`, travels by name: the executors load its namespace and look it up there, so the namespace has to be on their classpath, as it is in an application's uberjar. Any other function, such as a `(fn [x] ...)`, needs its class, so AOT-compile the namespace that defines it into the uberjar.
- **Over [Spark Connect](spark_connect.md).** The server runs the UDF, and Geni uploads what it needs to the session: Clojure's and Geni's jars, and the jars or source directories of the namespaces that the function uses. The server loads a namespace from its file, as a cluster's executors load it from the application's jar, so a var, or a function that a namespace in a file defines with `defn`, works as it is. A function defined at the REPL needs its class on the server, so connect with `{:keep-classes true}` and define the function after that: Clojure then writes the classes that it compiles to files, as `compile` does, for the whole JVM. Such a function can close over values, but the vars it calls have to come from namespaces in files. A session keeps the first version of each class and file that it gets, so a function redefined after it went to the session, or a file changed since, throws an error, and a new session from `g/connect` gets the new code. The first UDF on a session takes a few seconds, while the server loads Clojure and Geni, and so does each one that brings new classes. Geni's own tests run UDFs on a local session and on a Spark Connect server without Clojure.
- **Speed.** Spark can't look inside a UDF, and each value is converted on its way in and out, so a built-in function is usually faster. Reach for a UDF when there isn't one.
