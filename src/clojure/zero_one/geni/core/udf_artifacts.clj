(ns zero-one.geni.core.udf-artifacts
  "What a Spark Connect server needs to run a Clojure UDF, which goes up
  through the client's `addArtifact` once per session: Clojure's and Geni's
  jars, the jar or the source directory of each namespace that the function
  uses, and the classes that Clojure compiled for it at a REPL, which
  `keep-classes!` has it write to a directory."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string])
  (:import
   (clojure.lang DynamicClassLoader Var)
   (java.io File)
   (java.net URL)
   (java.nio.file Files)
   (java.nio.file.attribute FileAttribute)
   (java.util Collections WeakHashMap)
   (java.util.jar JarEntry JarOutputStream)))

;;;; Kept classes

(defonce ^:private kept-classes (atom nil))

(defn- set-var!
  "Sets the dynamic var for this thread, when it's bound here, as a REPL
  binds `*compile-path*`, and for the threads that don't bind it."
  [^Var v value]
  (alter-var-root v (constantly value))
  (when (thread-bound? v)
    (var-set v value)))

(defn keep-classes!
  "Has Clojure write the classes that it compiles from now on into a
  temporary directory, as `compile` does, so that a UDF from a function
  defined at a REPL can go to a Spark Connect server. It's on for this thread
  and the ones that don't bind `*compile-files*`. Returns the directory."
  ^File []
  (let [dir (or @kept-classes
                (reset! kept-classes
                        (.toFile (Files/createTempDirectory "geni-classes"
                                                            (make-array FileAttribute 0)))))]
    (set-var! #'*compile-path* (str dir))
    (set-var! #'*compile-files* true)
    dir))

;;;; Where code comes from

(defn- resource-path [ns-sym suffix]
  (str (-> (str ns-sym) (string/replace "-" "_") (string/replace "." "/")) suffix))

(defn- resource-root
  "The jar or the classpath directory that holds `resource`, or nil."
  ^File [^String resource]
  (when-let [^URL url (io/resource resource)]
    (case (.getProtocol url)
      "jar"  (io/file (.toURI (URL. (first (string/split (.getPath url) #"!/" 2)))))
      "file" (nth (iterate #(.getParentFile ^File %) (io/file (.toURI url)))
                  (count (string/split resource #"/")))
      nil)))

(defn- namespace-root
  "The jar or the classpath directory that holds the namespace's code."
  ^File [ns-sym]
  (some #(resource-root (resource-path ns-sym %)) ["__init.class" ".clj" ".cljc"]))

(defn- kept-files
  "The class files that Clojure kept for the namespace, as [file path], with
  the path relative to the directory."
  [ns-sym]
  (when-let [^File dir @kept-classes]
    (let [prefix (resource-path ns-sym "")
          base   (.toPath dir)]
      (for [^File f (file-seq dir)
            :when (.isFile f)
            :let [path (string/replace (str (.relativize base (.toPath f))) File/separator "/")]
            :when (and (string/ends-with? path ".class")
                       (or (string/starts-with? path (str prefix "$"))
                           (string/starts-with? path (str prefix "__init"))
                           (string/starts-with? path (str prefix "/"))
                           (= path (str prefix ".class"))))]
        [f path]))))

(defn- uses
  "The namespaces that a loaded namespace uses: its aliases', and those of
  the vars it refers to."
  [ns-sym]
  (when-let [n (find-ns ns-sym)]
    (set (concat (map ns-name (vals (ns-aliases n)))
                 (keep #(when (var? %) (ns-name (.ns ^Var %))) (vals (ns-refers n)))))))

(def ^:private base-resources
  "Resources from the jars, or directories, that every UDF needs: Clojure's,
  spec's, and Geni's code, Java classes and resources."
  ["clojure/core__init.class"
   "clojure/spec/alpha.clj"
   "clojure/core/specs/alpha.clj"
   "zero_one/geni/core/udf.clj"
   "zero_one/geni/udf/UdfFn.class"
   "spark-docs.edn"])

(defn- base-roots []
  (set (keep resource-root base-resources)))

(defn- own?
  "Whether the namespace comes in the jars that every UDF needs."
  [ns-sym]
  (contains? (base-roots) (namespace-root ns-sym)))

(defn- needed-namespaces
  "The namespaces that `f` uses, and theirs, but not Clojure's or Geni's."
  [f]
  (loop [todo (vec ((requiring-resolve 'zero-one.geni.rdd.function/namespace-references) f))
         seen #{}]
    (if-let [ns-sym (first todo)]
      (if (or (seen ns-sym) (own? ns-sym))
        (recur (subvec todo 1) seen)
        (recur (into (subvec todo 1) (uses ns-sym)) (conj seen ns-sym)))
      seen)))

(defonce ^:private dir-jars (atom {}))

(defn- jar-of-dir
  "A temporary jar of the classpath directory's files, made once."
  ^File [^File dir]
  (or (@dir-jars dir)
      (let [jar  (doto (File/createTempFile "geni-classpath-" ".jar") .deleteOnExit)
            base (.toPath dir)]
        (with-open [out (JarOutputStream. (io/output-stream jar))]
          (doseq [^File f (file-seq dir)
                  :when (.isFile f)]
            (.putNextEntry out (JarEntry. (string/replace (str (.relativize base (.toPath f)))
                                                          File/separator "/")))
            (io/copy f out)
            (.closeEntry out)))
        (get (swap! dir-jars assoc dir jar) dir))))

(defn- jar-for ^File [^File root]
  (if (.isDirectory root) (jar-of-dir root) root))

;;;; Uploads

(defonce ^:private uploaded
  ;; Each session's artifacts, so that each goes up once.
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- upload!
  "Uploads the artifact `k` to the session with `f`, unless it's there."
  [session k f]
  (let [done (or (.get ^java.util.Map uploaded session)
                 (let [s (java.util.concurrent.ConcurrentHashMap/newKeySet)]
                   (.put ^java.util.Map uploaded session s)
                   s))]
    (when-not (.contains done k)
      (f)
      (.add done k))))

(defn- upload-jar! [session ^File jar]
  (upload! session (str "jar:" jar) #(.addArtifact session (.getAbsolutePath jar))))

(defn- upload-class! [session [^File file path]]
  (upload! session (str "class:" path ":" (.lastModified file))
           #(.addArtifact session (.getAbsolutePath file) ^String path)))

(defn- compiled-at-run-time?
  "Whether `f`'s class came from Clojure's compiler in this JVM, rather than
  from a jar or a classpath directory."
  [f]
  (instance? DynamicClassLoader (.getClassLoader (class f))))

(defn- class-kept? [f]
  (let [path (str (string/replace (.getName (class f)) "." "/") ".class")]
    (some-> ^File @kept-classes (io/file path) .isFile)))

(defn- class-namespace
  "The namespace of a function's class, such as `user` for `user$eval1$fn__2`."
  [f]
  (symbol (first (string/split (string/replace (.getName (class f)) "_" "-") #"\$"))))

(defn- named-in-a-file?
  "Whether `f` is a top-level `defn` of a namespace in a file, whose class the
  server makes with the same name when it loads the namespace: `my.app$f`,
  where an anonymous function's has a number, as `my.app$f$fn__123` has."
  [f]
  (and (re-matches #"[^$]+\$[^$]+" (.getName (class f)))
       (not (re-find #"__\d+$" (.getName (class f))))
       (some? (namespace-root (class-namespace f)))))

(defn- upload-udf-artifacts! [session f]
  ;; A var goes to the server by name, and the server loads its namespace.
  ;; A function goes as an object, so the server needs its class: one that
  ;; Clojure kept, or one that the server makes as it loads the namespace.
  (when (and (not (var? f))
             (compiled-at-run-time? f)
             (not (class-kept? f))
             (not (named-in-a-file? f)))
    (throw (ex-info (str "This function was compiled before Geni connected to Spark Connect, or "
                         "with :keep-classes off, so its class can't go to the server. Define it "
                         "again, or reload its namespace, after g/connect, or pass a var of a "
                         "namespace that the server can load, such as #'my.app/my-fn.")
                    {:function (.getName (class f))})))
  (doseq [root (base-roots)]
    (upload-jar! session (jar-for root)))
  (doseq [ns-sym (into (needed-namespaces f)
                       [(if (var? f) (ns-name (.ns ^Var f)) (class-namespace f))])
          :when (not (own? ns-sym))]
    (if-let [kept (seq (kept-files ns-sym))]
      (run! #(upload-class! session %) kept)
      (when-let [root (namespace-root ns-sym)]
        (upload-jar! session (jar-for root))))))

(defn upload-udf!
  "Uploads to `session` what its Spark Connect server needs to run `f`, a
  function or a var, as a UDF, each artifact once: Clojure's and Geni's jars,
  the classes that Clojure kept for the namespaces that `f` uses, or else
  their jars or source directories, and `f`'s own class. Throws an error that
  says what to do when `f` was compiled at run time without its class kept."
  [session f]
  (upload! session (str "udf:" (.getName (class f)) "@" (System/identityHashCode f))
           #(upload-udf-artifacts! session f)))
