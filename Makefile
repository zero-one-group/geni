DOCKERNAME=zeroonetechnology/geni
VERSION=`cat resources/GENI_REPL_RELEASED_VERSION`

build:
	cp project.clj docker/project.clj
	docker build -f docker/Dockerfile \
		-t $(DOCKERNAME):$(VERSION) \
		-t $(DOCKERNAME):latest \
		docker

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
	docker run --rm -v $(PWD):/root/geni -w /root/geni -t $(DOCKERNAME) \
		lein midje :autotest

coverage: build
	$(eval TMP := $(shell mktemp -d))
	cp -r . $(TMP)
	docker run --rm -v $(TMP):/root/geni -w /root/geni -t $(DOCKERNAME) \
		scripts/coverage

lint-ancient: build
	$(eval TMP := $(shell mktemp -d))
	cp -r . $(TMP)
	docker run --rm -v $(TMP):/root/geni -w /root/geni -t $(DOCKERNAME) \
		scripts/lint-ancient

test-geni-cli: build
	$(eval TMP := $(shell mktemp -d))
	cp -r . $(TMP)
	docker run --rm -v $(TMP):/root/geni -w /root/geni -t $(DOCKERNAME) \
		scripts/test-geni-cli

test-lein-template: build
	$(eval TMP := $(shell mktemp -d))
	cp -r . $(TMP)
	docker run --rm -v $(TMP):/root/geni -w /root/geni -t $(DOCKERNAME) \
		scripts/test-lein-template

test-install-geni-cli: build
	$(eval TMP := $(shell mktemp -d))
	cp -r . $(TMP)
	docker run --rm -v $(TMP):/root/geni -w /root/geni -t $(DOCKERNAME) \
		scripts/test-install-geni-cli

ci: coverage test-install-geni-cli test-geni-cli test-lein-template lint-ancient 
	echo "CI steps passed!"
