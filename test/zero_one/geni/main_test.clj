(ns zero-one.geni.main-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.repl :as repl]
    [zero-one.geni.main :as main]))

(facts "On repl"
  (fact "correct prompts"
    (repl/geni-prompt "xyz") => "geni-repl (xyz)\nλ ")

  (fact "correct welcome note"
    (repl/spark-welcome-note (.version main/spark))
    => #(clojure.string/includes? % "spark"))

  (fact "correct nREPL connection"
    (let [port (+ 65001 (rand-int 500))]
      (with-out-str (repl/launch-repl {:port port :dummy true}))
      => (str "nREPL server started on port " port "\n")))

  (fact "correct nREPL connection with options"
    (let [port (+ 65001 (rand-int 500))]
      (with-out-str (repl/launch-repl {:port port
                                       :host "localhost"
                                       :dummy true}))
      => (str "nREPL server started on port " port "\n"))))

(facts "On main"
  (with-out-str (main/-main :dummy))
  => #(clojure.string/includes? % ":spark.master"))
