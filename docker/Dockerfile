FROM clojure:openjdk-11-lein-2.9.1

RUN apt-get update && apt-get install -y libgomp1 libatlas3-base libopenblas-base

ENV USER root
ENV HOME /root

ADD spark-project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN bash -c "bash <(curl -s https://raw.githubusercontent.com/borkdude/clj-kondo/master/script/install-clj-kondo)"

ADD project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN cd /root/ && lein cloverage || true

ENTRYPOINT []
