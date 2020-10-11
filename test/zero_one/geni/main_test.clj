(ns zero-one.geni.main-test
  (:require
   [clojure.string]
   [midje.sweet :refer [facts fact =>]]
   [zero-one.geni.repl :as repl]
   [zero-one.geni.test-resources :refer [spark]]))

(defn exit-stream []
  (-> "exit\n" (.getBytes "UTF-8") (java.io.ByteArrayInputStream.)))

(defn nrepl-message [port]
  (str "nREPL server started on port " port "\n"))

(facts "On repl" :repl
  (fact "correct prompts"
    (repl/geni-prompt "xyz") => "geni-repl (xyz)\nλ ")

  (fact "correct welcome note"
    (repl/spark-welcome-note (.version spark))
    => #(clojure.string/includes? % "spark"))

  (fact "correct nREPL connection"
    (let [port (+ 65001 (rand-int 500))]
      (with-out-str (repl/launch-repl {:port port
                                       :input-stream (exit-stream)}))
      => #(clojure.string/includes? % (nrepl-message port))))

  (fact "correct nREPL connection with options"
    (let [port (+ 65001 (rand-int 500))]
      (with-out-str (repl/launch-repl {:port port
                                       :host "localhost"
                                       :input-stream (exit-stream)}))
      => #(clojure.string/includes? % (nrepl-message port)))))
