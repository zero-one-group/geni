(ns build
  "Build tasks. Run them with `clojure -T:build <task>`."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.tools.build.api :as b]
   [deps-deploy.deps-deploy :as dd]))

(def lib 'zero.one/geni)
(def version "0.4.0")

(def class-dir "target/classes")
(def test-class-dir "target/test-classes")
(def jar-class-dir "target/jar")
(def uber-class-dir "target/uber")
(def jar-file (format "target/geni-%s.jar" version))
;; The name that scripts/geni downloads from the GitHub release.
(def uber-file (format "target/geni-repl-uberjar-%s.jar" version))

;; resources/GENI_REPL_RELEASED_VERSION is only there for scripts/geni, which
;; reads it from GitHub, so it stays out of the jars.
(def ^:private ignores
  [".*~$" "^#.*#$" "^\\.#.*" "^.DS_Store$" "^GENI_REPL_RELEASED_VERSION$"])

(defn- basis [& aliases]
  (b/create-basis {:project "deps.edn" :aliases (vec aliases)}))

(defn- alias-deps
  "The :extra-deps of the given deps.edn aliases."
  [& aliases]
  (let [deps-edn (edn/read-string (slurp "deps.edn"))]
    (into {} (map #(get-in deps-edn [:aliases % :extra-deps])) aliases)))

(defn- add-opens
  "The --add-opens flags of the :spark alias, as a jar manifest's Add-Opens,
  so that `java -jar` doesn't need them."
  []
  (let [deps-edn (edn/read-string (slurp "deps.edn"))]
    (->> (get-in deps-edn [:aliases :spark :jvm-opts])
         (keep #(second (re-matches #"--add-opens=(.+)=ALL-UNNAMED" %)))
         (interpose " ")
         (apply str))))

(defn- javac! [target-dir]
  (b/javac {:src-dirs   ["src/java"]
            :class-dir  target-dir
            :basis      (basis :spark)
            :javac-opts ["--release" "17" "-proc:none"]}))

(defn- lib-basis
  "The basis for the library's pom: Clojure, plus the :spark alias's deps
  marked optional. Maven, Leiningen and tools.deps don't pull optional deps
  in, so users still bring their own Spark, but cljdoc puts them on its
  classpath, which it needs to load Geni's namespaces."
  []
  (let [root (basis)]
    (assoc root :libs (merge (:libs root)
                             (update-vals (alias-deps :spark) #(assoc % :optional true))))))

(defn- commit
  "The commit being built. The pom records it as its SCM tag, so that
  release.yml can tell whether Clojars has this version from another commit."
  []
  (b/git-process {:git-args ["rev-parse" "HEAD"]}))

(defn- write-pom! [{:keys [lib class-dir basis src-dirs description]}]
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     basis
                :src-dirs  src-dirs
                ;; Never start from a stray pom.xml in the repo root.
                :src-pom   :none
                :scm       {:url                 "https://github.com/zero-one-group/geni"
                            :connection          "scm:git:git://github.com/zero-one-group/geni.git"
                            :developerConnection "scm:git:ssh://git@github.com/zero-one-group/geni.git"
                            :tag                 (commit)}
                :pom-data  [[:description description]
                            [:url "https://github.com/zero-one-group/geni"]
                            [:licenses
                             [:license
                              [:name "Apache-2.0"]
                              [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]}))

(defn clean
  "Deletes the target directory."
  [_]
  (b/delete {:path "target"}))

(defn compile-java
  "Compiles src/java into target/classes."
  [_]
  (javac! class-dir))

(defn- spark-build
  "The Spark and Scala versions in a basis, from its spark-core."
  [basis]
  (some (fn [[lib coord]]
          (when-let [[_ scala] (re-matches #"spark-core_(2\.\d+)" (name lib))]
            {:spark (:mvn/version coord) :scala scala}))
        (:libs basis)))

(defn prep
  "Starts from a clean target/, then compiles the Java sources, plus the
  namespaces that the RDD tests need ahead of time. Run it after cloning, after
  changing src/java, and after switching to another Spark alias, e.g.
  `clojure -T:build prep :spark :spark-4`. The Java is always compiled against
  :spark, as it is for the jar."
  [{:keys [spark] :or {spark :spark}}]
  (clean nil)
  (compile-java nil)
  (let [basis (basis spark :test)]
    (b/compile-clj {:basis      basis
                    :ns-compile '[zero-one.geni.rdd.function
                                  zero-one.geni.aot-functions]
                    :class-dir  test-class-dir})
    ;; The test runner checks it against the Spark that the tests run on.
    (spit (str test-class-dir "/spark.edn") (pr-str (spark-build basis)))))

(defn- sh!
  "Runs a command in the repo, and exits with its status if it fails."
  [& args]
  (let [{:keys [exit]} (b/process {:command-args (vec args)})]
    (when-not (zero? exit)
      (binding [*out* *err*]
        (println "Failed:" (string/join " " args)))
      (System/exit exit))))

(def ^:private lint-paths ["src" "test/zero_one" "cli" "test-tmd" "test-xgb" "test-graph" "dev" "build.clj"])
(def ^:private fmt-paths ["src" "test" "cli" "test-tmd" "test-xgb" "test-graph" "dev" "build.clj"])

(def ^:private lint-commands
  [(into ["clojure" "-M:kondo" "--lint"] lint-paths)
   (into ["clojure" "-M:fmt" "check"] fmt-paths)])

(defn lint
  "Runs clj-kondo, then cljfmt's check, as the CI does."
  [_]
  (run! #(apply sh! %) lint-commands))

(defn fmt
  "Reformats the sources with cljfmt."
  [_]
  (apply sh! "clojure" "-M:fmt" "fix" fmt-paths))

(def ^:private doc-commands
  [["clojure" "-X:gen-doc-tests"]
   ["clojure" "-X:spark:test:tmd:doc-tests"]])

(defn docs
  "Runs the Clojure blocks in the README and docs/ as tests, on :spark, with
  tech.ml.dataset for the collecting guide. Run `prep` first."
  [_]
  (run! #(apply sh! %) doc-commands))

(defn cookbook
  "Runs the cookbook's Clojure blocks as tests, on :spark, as the weekly
  workflow does. The first run downloads the datasets into data/cookbook. Run
  `prep` first."
  [_]
  (sh! "clojure" "-X:gen-doc-tests"
       ":docs" "[\"docs/cookbook/*.md\"]"
       ":target-root" "\"target/cookbook\"")
  (sh! "clojure" "-X:spark:test:cookbook"))

(defn xgb-tests
  "Runs the XGBoost tests in test-xgb/, then the XGBoost guide's examples,
  with XGBoost4J-Spark on the classpath: :xgb on :spark, and :xgb-2.13 on the
  Spark builds on Scala 2.13, as in `clojure -T:build xgb-tests :spark :spark-4`.
  Run `prep` with the same Spark first."
  [{:keys [spark] :or {spark :spark}}]
  (let [xgb     (if (= spark :spark) ":xgb" ":xgb-2.13")
        aliases (str "-X:" (name spark) ":test" xgb)]
    (sh! "clojure" aliases ":dirs" "[\"test-xgb\"]")
    (sh! "clojure" "-X:gen-doc-tests"
         ":docs" "[\"docs/xgboost.md\"]"
         ":target-root" "\"target/xgb-docs\"")
    (sh! "clojure" (str aliases ":xgb-docs"))))

(defn graph-tests
  "Runs the GraphFrames tests in test-graph/, then the graphs guide's
  examples, with GraphFrames on the classpath: :graphframes on :spark, and
  :graphframes-4 on :spark-4, as in `clojure -T:build graph-tests :spark
  :spark-4`. Run `prep` with the same Spark first."
  [{:keys [spark] :or {spark :spark}}]
  (let [graphframes (if (= spark :spark-4) ":graphframes-4" ":graphframes")
        aliases     (str "-X:" (name spark) ":test" graphframes)]
    (sh! "clojure" aliases ":dirs" "[\"test-graph\"]")
    (sh! "clojure" "-X:gen-doc-tests"
         ":docs" "[\"docs/graphs.md\"]"
         ":target-root" "\"target/graph-docs\"")
    (sh! "clojure" (str aliases ":graph-docs"))))

(def ^:private azure-env
  "What the cloud storage guide's examples read: the storage account, a
  container in it and the account's key."
  ["AZURE_STORAGE_ACCOUNT" "AZURE_STORAGE_CONTAINER" "AZURE_STORAGE_KEY"])

(def ^:private azure-service-principal-env
  ["AZURE_TENANT_ID" "AZURE_CLIENT_ID" "AZURE_CLIENT_SECRET"])

(defn cloud-docs
  "Runs the cloud storage guide's examples against a real Azure storage
  account, from AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_CONTAINER and
  AZURE_STORAGE_KEY, and its service principal's example too when
  AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET are set. They
  write under geni-docs/ in the container. On :spark with :azure, or on
  :spark-4 with :azure-4, as in `clojure -T:build cloud-docs :spark :spark-4`.
  The CI doesn't run it. Run `prep` with the same Spark first."
  [{:keys [spark] :or {spark :spark}}]
  (when-let [missing (seq (remove #(System/getenv %) azure-env))]
    (println "Set" (string/join ", " missing) "first.")
    (System/exit 1))
  (let [azure   (if (= spark :spark-4) ":azure-4" ":azure")
        aliases (str "-X:" (name spark) ":test" azure ":cloud-docs")]
    (sh! "clojure" "-X:gen-doc-tests"
         ":docs" "[\"docs/cloud_storage.md\"]"
         ":target-root" "\"target/cloud-docs\"")
    (apply sh! "clojure" aliases
           (when-not (every? #(System/getenv %) azure-service-principal-env)
             [":exclude" ":azure-sp"]))))

(defn- run-all!
  "Runs the commands one after another, until one fails, with their output,
  stderr included, appended to `file`. Returns the last exit code."
  [file commands]
  (reduce (fn [_ args]
            (let [exit (-> (doto (ProcessBuilder. ^java.util.List args)
                             (.redirectErrorStream true)
                             (.redirectOutput (java.lang.ProcessBuilder$Redirect/appendTo file)))
                           .start
                           .waitFor)]
              (if (zero? exit) 0 (reduced exit))))
          0
          commands))

(defn- in-background
  "Runs the commands as `run-all!` does, in the background, with their output
  in the file `out`. Returns a future of the exit code and the output."
  [out commands]
  (let [file (io/file out)]
    (io/make-parents file)
    (spit file "")
    (future (let [exit (run-all! file commands)]
              {:exit exit :out (slurp file)}))))

(defn check
  "Lints, runs the tests and runs the doc tests, on :spark: what the CI runs
  on a pull request. The lint and the doc tests run alongside the tests, and
  their output follows the tests'. Run `prep` first."
  [_]
  (let [lint-run (in-background "target/lint.out" lint-commands)
        ;; With a log of their own, since the tests write target/test.log.
        docs-run (in-background "target/docs.out"
                                (update doc-commands 1 conj ":log" "\"target/doc-tests.log\""))
        tests    (:exit (b/process {:command-args ["clojure" "-X:spark:test:cli:tmd"]}))
        {lint-exit :exit lint-out :out} @lint-run
        {docs-exit :exit docs-out :out} @docs-run
        failed   (keep (fn [[step exit]] (when (pos? exit) step))
                       [["lint" lint-exit] ["the tests" tests] ["the doc tests" docs-exit]])]
    (print (str "\n" lint-out "\n" docs-out))
    (flush)
    (shutdown-agents)
    (when (seq failed)
      (println "\nFailed:" (string/join ", " failed))
      (System/exit 1))))

(defn- port-open? [port]
  (try
    (with-open [_ (java.net.Socket. "127.0.0.1" (int port))] true)
    (catch java.io.IOException _ false)))

(defn- exit-code [command-args env]
  (:exit (b/process {:command-args command-args :env env})))

(defn- without-clojure
  "The basis without Clojure's jars and the project's own paths, so that a
  Spark Connect server on it has never heard of Clojure, as a real one hasn't,
  and the UDF tests check what Geni uploads to it."
  [basis]
  (update basis :classpath-roots
          (fn [roots]
            (remove (fn [root]
                      (let [{:keys [lib-name path-key]} (get-in basis [:classpath root])]
                        (or path-key (= "org.clojure" (some-> lib-name namespace)))))
                    roots))))

(defn- with-connect-server
  "Starts a Spark Connect server on :spark-4 in the background, on 127.0.0.1
  at `port`, with its log in target/connect-server.log, and without Clojure.
  Once it's up, calls `f` with the environment that points a client at it,
  then stops the server. Returns an exit code, as `f` does."
  [port f]
  (let [log     (io/file "target/connect-server.log")
        command (:command-args
                 (b/java-command
                  {:basis     (without-clojure (basis :spark-4 :test :connect-server))
                   :main      'org.apache.spark.sql.connect.service.SparkConnectServer
                   :java-opts ["-Dspark.master=local[*]"
                               "-Dspark.connect.grpc.binding.address=127.0.0.1"
                               (str "-Dspark.connect.grpc.binding.port=" port)
                               "-Dspark.sql.warehouse.dir=target/connect-warehouse"
                               ;; For reliable checkpoints, which a Connect client
                               ;; can't give a directory.
                               "-Dspark.checkpoint.dir=target/connect-checkpoints"]}))
        _       (io/make-parents log)
        server  (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectErrorStream true)
                          (.redirectOutput log)))
        ;; In case the build is killed before the finally below.
        hook    (Thread. #(.destroyForcibly server))]
    (.addShutdownHook (Runtime/getRuntime) hook)
    (try
      (loop [waited 0]
        (cond
          (port-open? port)
          (f {"SPARK_REMOTE" (str "sc://127.0.0.1:" port)})

          (or (not (.isAlive server)) (> waited 120000))
          (do (println "The Spark Connect server didn't start. See" (str log))
              1)

          :else
          (do (Thread/sleep 500)
              (recur (+ waited 500)))))
      (finally
        (.destroy server)
        (when-not (.waitFor server 30 java.util.concurrent.TimeUnit/SECONDS)
          (.destroyForcibly server)
          (.waitFor server))
        (.removeShutdownHook (Runtime/getRuntime) hook)))))

(defn connect-tests
  "Runs the tests over Spark Connect: it starts a Spark Connect server on
  :spark-4 in the background, on 127.0.0.1 at `:port` (15002 by default), then
  runs the tests with Spark's JVM client in place of classic Spark, which
  skips the ones marked ^:classic, then the tech.ml.dataset tests in
  test-tmd/, with Apache Arrow added, and the Spark Connect guide's examples.
  Other options go to the test runner instead, e.g.
  `:only '[zero-one.geni.dataset-test]'`. They skip test-tmd/ and the guide,
  whose examples connect to port 15002, and so does another port. Run
  `prep :spark :spark-4` first."
  [{:keys [port] :or {port 15002} :as opts}]
  (when (port-open? port)
    (println "Port" port "is taken. Stop what's on it, or pass another :port.")
    (System/exit 1))
  (let [args (mapcat (fn [[k v]] [(str k) (pr-str v)]) (dissoc opts :port))
        exit (with-connect-server
               port
               (fn [env]
                 (let [tests (exit-code (into ["clojure" "-X:spark-connect:test"] args) env)]
                   (if (or (seq args) (not= 15002 port) (pos? tests))
                     tests
                     (let [tmd (exit-code ["clojure" "-X:spark-connect:test:tmd:connect-arrow"
                                           ":dirs" "[\"test-tmd\"]"]
                                          env)
                           gen (when (zero? tmd)
                                 (exit-code ["clojure" "-X:gen-doc-tests"
                                             ":docs" "[\"docs/spark_connect.md\"]"
                                             ":target-root" "\"target/connect-docs\""]
                                            {}))]
                       (cond
                         (pos? tmd) tmd
                         (pos? gen) gen
                         :else      (exit-code ["clojure" "-X:spark-connect:test:connect-docs"] env)))))))]
    (when-not (zero? exit)
      (System/exit exit))))

(defn check-all
  "Everything that `check` runs, plus the tests on :spark-4 and over Spark
  Connect, and the GraphFrames tests: lint, then the tests, `graph-tests` and
  `connect-tests` on :spark-4, then the tests, `graph-tests` and the doc
  tests on :spark. It preps each Spark itself, and stops at the first
  failure. It ends prepped for :spark, as `check` expects. The XGBoost tests
  and the cookbook stay separate, as `xgb-tests` and `cookbook`."
  [_]
  (lint nil)
  (prep {:spark :spark-4})
  (sh! "clojure" "-X:spark-4:test:cli:tmd")
  (graph-tests {:spark :spark-4})
  (connect-tests {})
  (prep {})
  (sh! "clojure" "-X:spark:test:cli:tmd")
  (graph-tests {})
  (docs nil))

(defn jar
  "Builds the library jar into target/."
  [_]
  (b/delete {:path jar-class-dir})
  (javac! jar-class-dir)
  (write-pom! {:lib         lib
               :class-dir   jar-class-dir
               :basis       (lib-basis)
               :src-dirs    ["src/clojure"]
               :description "A Clojure dataframe library that runs on Apache Spark"})
  (b/copy-dir {:src-dirs   ["src/clojure" "resources"]
               :target-dir jar-class-dir
               :ignores    ignores})
  (b/jar {:class-dir jar-class-dir :jar-file jar-file}))

(defn install
  "Installs the jar into the local Maven repo."
  [_]
  (jar nil)
  (b/install {:basis     (basis)
              :lib       lib
              :version   version
              :jar-file  jar-file
              :class-dir jar-class-dir}))

(defn deploy
  "Deploys the jar to Clojars, using CLOJARS_USERNAME and CLOJARS_PASSWORD."
  [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact  (b/resolve-path jar-file)
              :pom-file  (b/pom-path {:lib lib :class-dir jar-class-dir})}))

(defn cli-uber
  "Builds the Geni CLI uberjar into target/, from cli/src and cli/resources
  (its log4j2 config), with Geni, Spark and the namespaces compiled ahead of
  time. Run it with `java -jar`. The CLI has no Clojars artifact: Clojars no
  longer accepts new libraries in a group it can't verify, and zero.one isn't
  a domain."
  [_]
  (b/delete {:path uber-class-dir})
  (let [basis (basis :spark :cli)]
    (javac! uber-class-dir)
    (b/copy-dir {:src-dirs   ["src/clojure" "resources" "cli/src" "cli/resources"]
                 :target-dir uber-class-dir
                 :ignores    ignores})
    (b/compile-clj {:basis      basis
                    :ns-compile '[zero-one.geni.main]
                    :class-dir  uber-class-dir})
    (b/uber {:class-dir uber-class-dir
             :uber-file uber-file
             :basis     basis
             :main      'zero-one.geni.main
             :manifest  {"Add-Opens" (add-opens)}})))
