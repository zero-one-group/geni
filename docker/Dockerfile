FROM clojure:openjdk-11-lein-2.9.1

ENV USER root
ENV HOME /root

# Install other dependencies
RUN apt-get update && apt-get install -y libgomp1 libatlas3-base libopenblas-base

# Install clj-kondo and babashka
RUN bash -c "bash <(curl -s https://raw.githubusercontent.com/borkdude/clj-kondo/master/script/install-clj-kondo)"
RUN  curl -s https://raw.githubusercontent.com/borkdude/babashka/master/install -o install-babashka
RUN chmod +x install-babashka && ./install-babashka

# Cache big, main dependencies such as Apache Spark and databases
ADD spark-project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

# Install other dependencies and plugins
ADD project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN cd /root/ && lein cloverage || true

ENTRYPOINT []
