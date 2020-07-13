FROM clojure:openjdk-11-lein-2.9.1

RUN apt-get update && apt-get install libgomp1

ENV USER root
ENV HOME /root

ADD spark-project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN wget -O clj-kondo.zip https://github.com/borkdude/clj-kondo/releases/download/v2020.06.21/clj-kondo-2020.06.21-linux-amd64.zip &&\
    unzip -qqo clj-kondo.zip && \
    rm clj-kondo.zip && \
    mv clj-kondo /usr/local/bin/

ADD project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN cd /root/ && lein cloverage || true

ENTRYPOINT []
