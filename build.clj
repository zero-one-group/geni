(ns build
  "Build tasks. Run them with `clojure -T:build <task>`."
  (:require
   [clojure.tools.build.api :as b]
   [deps-deploy.deps-deploy :as dd]))

(def lib 'zero.one/geni)
(def version "0.1.0-alpha.1")

(def class-dir "target/classes")
(def test-class-dir "target/test-classes")
(def jar-class-dir "target/jar")
(def jar-file (format "target/geni-%s.jar" version))

(defn- basis [& aliases]
  (b/create-basis {:project "deps.edn" :aliases (vec aliases)}))

(defn- javac! [target-dir]
  (b/javac {:src-dirs   ["src/java"]
            :class-dir  target-dir
            :basis      (basis :spark)
            :javac-opts ["--release" "17" "-proc:none"]}))

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
  (b/write-pom {:class-dir jar-class-dir
                :lib       lib
                :version   version
                :basis     (basis)
                :src-dirs  ["src/clojure"]
                :scm       {:url                 "https://github.com/zero-one-group/geni"
                            :connection          "scm:git:git://github.com/zero-one-group/geni.git"
                            :developerConnection "scm:git:ssh://git@github.com/zero-one-group/geni.git"
                            :tag                 (str "v" version)}
                :pom-data  [[:description "A Clojure dataframe library that runs on Apache Spark"]
                            [:url "https://github.com/zero-one-group/geni"]
                            [:licenses
                             [:license
                              [:name "Apache-2.0"]
                              [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
  (b/copy-dir {:src-dirs   ["src/clojure" "resources"]
               :target-dir jar-class-dir})
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
