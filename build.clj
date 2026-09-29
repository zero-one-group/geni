(ns build
  "Build tasks. Run them with `clojure -T:build <task>`."
  (:require
   [clojure.edn :as edn]
   [clojure.tools.build.api :as b]
   [deps-deploy.deps-deploy :as dd]))

(def lib 'zero.one/geni)
(def cli-lib 'zero.one/geni-cli)
(def version "0.1.0-alpha.1")

(def class-dir "target/classes")
(def test-class-dir "target/test-classes")
(def jar-class-dir "target/jar")
(def cli-class-dir "target/cli")
(def uber-class-dir "target/uber")
(def jar-file (format "target/geni-%s.jar" version))
(def cli-jar-file (format "target/geni-cli-%s.jar" version))
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
                            :tag                 (str "v" version)}
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

(defn prep
  "Starts from a clean target/, then compiles the Java sources, plus the
  namespaces that the RDD tests need ahead of time. Run it after cloning, and
  after changing src/java."
  [_]
  (clean nil)
  (compile-java nil)
  (b/compile-clj {:basis      (basis :spark :test)
                  :ns-compile '[zero-one.geni.rdd.function
                                zero-one.geni.aot-functions]
                  :class-dir  test-class-dir}))

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

(defn cli-jar
  "Builds the Geni CLI jar, zero.one/geni-cli, into target/. It holds cli/src,
  and depends on zero.one/geni, Spark (the :spark alias) and the :cli deps."
  [_]
  (b/delete {:path cli-class-dir})
  ;; write-pom only reads a basis's :libs and :mvn/repos, so this one isn't
  ;; resolved, and this version of zero.one/geni needn't be on Clojars yet.
  (write-pom! {:lib         cli-lib
               :class-dir   cli-class-dir
               :basis       (assoc (select-keys (basis) [:mvn/repos])
                                   :libs (assoc (alias-deps :spark :cli) lib {:mvn/version version}))
               :src-dirs    ["cli/src"]
               :description "The Geni CLI: a Clojure REPL and an nREPL server, with Geni and Spark loaded"})
  (b/copy-dir {:src-dirs ["cli/src"] :target-dir cli-class-dir})
  (b/jar {:class-dir cli-class-dir :jar-file cli-jar-file}))

(defn cli-deploy
  "Deploys the Geni CLI jar to Clojars. Deploy the library first, since the CLI
  depends on this version of it."
  [_]
  (cli-jar nil)
  (dd/deploy {:installer :remote
              :artifact  (b/resolve-path cli-jar-file)
              :pom-file  (b/pom-path {:lib cli-lib :class-dir cli-class-dir})}))

(defn cli-uber
  "Builds the Geni CLI uberjar into target/, with Geni, Spark and the
  namespaces compiled ahead of time. Run it with `java -jar`."
  [_]
  (b/delete {:path uber-class-dir})
  (let [basis (basis :spark :cli)]
    (javac! uber-class-dir)
    (b/copy-dir {:src-dirs   ["src/clojure" "resources" "cli/src"]
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
