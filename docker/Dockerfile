FROM clojure:openjdk-8-lein-2.9.1

ENV USER root
ENV HOME /root

ADD project.clj /root/project.clj
RUN cd /root && lein with-profile +dev deps

RUN bash -c "bash <(curl -s https://raw.githubusercontent.com/borkdude/clj-kondo/master/script/install-clj-kondo)"

ENTRYPOINT []
