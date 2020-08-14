DOCKERNAME=zeroonetechnology/geni

build:
	cp project.clj docker/project.clj
	docker build -f docker/Dockerfile -t $(DOCKERNAME) docker

docker-pull:
	docker pull $(DOCKERNAME)

dock: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		/bin/bash

repl: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein repl

autotest: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein midje :autotest

docker-push: build
	docker push $(DOCKERNAME)

coverage: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein coverage

lint: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		clj-kondo --lint src/clojure/zero_one test/zero_one --cache false

template-test: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-template

install-geni-test: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-install-geni

geni-cli-test: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-geni

pre-release-test: coverage lint geni-cli-test template-test install-geni-test
