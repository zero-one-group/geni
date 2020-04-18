DOCKERNAME=zeroonetechnology/geni

build:
	cp project.clj docker/project.clj
	docker build -f docker/Dockerfile -t $(DOCKERNAME) docker

dock: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		/bin/bash

repl: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein repl

unit-tests: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein midje

autotest: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein midje :autotest

coverage:
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		lein cloverage --fail-threshold 90

docker-push:
	docker push $(DOCKERNAME)
