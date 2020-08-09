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
		clj-kondo --lint src test --cache false

template-test: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		bash -c "cd lein-template && lein new geni temporary && cd temporary && lein test"

install-geni-test: build
	docker run --rm -v $(PWD):/root/geni -w /root/geni -it $(DOCKERNAME) \
		scripts/test-install-geni


ci: coverage lint
