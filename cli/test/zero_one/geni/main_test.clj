(ns zero-one.geni.main-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.repl :as repl]
   [zero-one.geni.test-resources :refer [spark]]))

(defn exit-stream []
  (-> "exit\n" (.getBytes "UTF-8") (java.io.ByteArrayInputStream.)))

(defn nrepl-message [port]
  (str "nREPL server started on port " port "\n"))

(deftest repl-test
  (testing "correct prompts"
    (is (= "geni-repl (xyz)\nλ " (repl/geni-prompt "xyz"))))

  (testing "correct welcome note"
    (is (clojure.string/includes? (repl/spark-welcome-note (.version @spark)) "spark")))

  (testing "correct nREPL connection"
    (let [port (+ 65001 (rand-int 500))]
      (is (clojure.string/includes? (with-out-str (repl/launch-repl {:port port
                                                                     :input-stream (exit-stream)})) (nrepl-message port)))))

  (testing "correct nREPL connection with options"
    (let [port (+ 65001 (rand-int 500))]
      (is (clojure.string/includes? (with-out-str (repl/launch-repl {:port port
                                                                     :host "localhost"
                                                                     :input-stream (exit-stream)})) (nrepl-message port))))))
