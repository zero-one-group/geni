DOCKERNAME=zeroonetechnology/geni
VERSION=`cat resources/GENI_REPL_RELEASED_VERSION`

build:
	cp project.clj docker/project.clj
	docker build -f docker/Dockerfile -t $(DOCKERNAME):$(VERSION) docker

docker-pull:
	docker pull $(DOCKERNAME):$(VERSION)

docker-push: build
	docker push $(DOCKERNAME):$(VERSION)

dock: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		/bin/bash

repl: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein repl

autotest: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein midje :autotest

coverage: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/coverage

lint-ancient: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/lint-ancient

test-geni-cli: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-geni-cli

test-lein-template: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-lein-template

test-install-geni-cli: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-install-geni-cli

ci: coverage lint-ancient test-geni-cli test-lein-template test-install-geni-cli
	lein clean
