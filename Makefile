PATH_COHERENCE = github.com/SimonRichardson/coherence

UNAME_S := $(shell uname -s)
SED ?= sed -i
ifeq ($(UNAME_S),Darwin)
	SED += '' --
endif

.PHONY: all
all: install
	# $(MAKE) clean build

.PHONY: install
install:
	go get github.com/Masterminds/glide
	go get github.com/mattn/goveralls
	go get github.com/golang/mock/mockgen
	go get github.com/prometheus/client_golang/prometheus
	glide install --strip-vendor

.PHONY: build
build: dist/coherence

dist/coherence:
	go build -o dist/coherence ${PATH_COHERENCE}/cmd/coherence

pkg/api/mocks/transport.go:
	mockgen -package=mocks -destination=pkg/api/mocks/transport.go ${PATH_COHERENCE}/pkg/api Transport,TransportStrategy
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/api/mocks/transport.go

pkg/cluster/mocks/peer.go:
	mockgen -package=mocks -destination=pkg/cluster/mocks/peer.go ${PATH_COHERENCE}/pkg/cluster Peer
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/cluster/mocks/peer.go

pkg/cluster/farm/mocks/farm.go:
	mockgen -package=mocks -destination=pkg/cluster/farm/mocks/farm.go ${PATH_COHERENCE}/pkg/cluster/farm Farm
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/cluster/farm/mocks/farm.go

pkg/cluster/hashring/mocks/hashring.go:
	mockgen -package=mocks -destination=pkg/cluster/hashring/mocks/hashring.go ${PATH_COHERENCE}/pkg/cluster/hashring Snapshot
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/cluster/hashring/mocks/hashring.go

pkg/cluster/members/mocks/members.go:
	mockgen -package=mocks -destination=pkg/cluster/members/mocks/members.go ${PATH_COHERENCE}/pkg/cluster/members Members,MemberList,Member
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/cluster/members/mocks/members.go

pkg/cluster/nodes/mocks/node.go:
	mockgen -package=mocks -destination=pkg/cluster/nodes/mocks/node.go ${PATH_COHERENCE}/pkg/cluster/nodes Node
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/cluster/nodes/mocks/node.go

pkg/metrics/mocks/metrics.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/metrics.go ${PATH_COHERENCE}/pkg/metrics Gauge,HistogramVec,Counter
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/metrics/mocks/metrics.go

pkg/metrics/mocks/observer.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/observer.go github.com/prometheus/client_golang/prometheus Observer
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/metrics/mocks/observer.go

pkg/store/mocks/store.go:
	mockgen -package=mocks -destination=pkg/store/mocks/store.go ${PATH_COHERENCE}/pkg/store Store
	@ $(SED) 's/github.com\/SimonRichardson\/coherence\/vendor\///g' ./pkg/store/mocks/store.go

.PHONY: build-mocks
build-mocks: pkg/api/mocks/transport.go \
	pkg/cluster/mocks/peer.go \
	pkg/cluster/farm/mocks/farm.go \
	pkg/cluster/hashring/mocks/hashring.go \
	pkg/cluster/members/mocks/members.go \
	pkg/cluster/nodes/mocks/node.go \
	pkg/metrics/mocks/metrics.go \
	pkg/metrics/mocks/observer.go \
	pkg/store/mocks/store.go

.PHONY: clean-mocks
clean-mocks: FORCE
	@ find . -type d -name 'mocks' -exec find {} -name '*.go' -delete -print \;

.PHONY: clean
clean: FORCE
	rm -f dist/coherence

FORCE:

.PHONY: unit-tests
unit-tests:
	docker-compose run coherence go test -v ./pkg/...

.PHONY: integration-tests
integration-tests:
	docker-compose run coherence go test -v -tags=integration ./pkg/...

.PHONY: documentation
documentation:
	go test -v -tags=documentation ./pkg/... -run=TestDocumentation_

.PHONY: coverage-tests
coverage-tests:
	docker-compose run coherence go test -covermode=count -coverprofile=bin/coverage.out -v -tags=integration ${COVER_PKG}

.PHONY: coverage-view
coverage-view:
	go tool cover -html=bin/coverage.out

.PHONY: coverage
coverage:
	docker-compose run -e TRAVIS_BRANCH=${TRAVIS_BRANCH} -e GIT_BRANCH=${GIT_BRANCH} \
		coherence \
		/bin/sh -c 'apk update && apk add make && apk add git && \
		go get github.com/mattn/goveralls && \
		/go/bin/goveralls -repotoken=${COVERALLS_REPO_TOKEN} -ignore=pkg/*/mocks/*.go -package=./pkg/... -flags=--tags=integration -service=travis-ci'

PWD ?= ${GOPATH}/src/${PATH_COHERENCE}
TAG ?= dev
BRANCH ?= dev
ifeq ($(BRANCH),master)
	TAG=latest
endif

.PHONY: build-docker
build-docker:
	@echo "Building '${TAG}' for '${BRANCH}'"
	docker run --rm -v ${PWD}:/go/src/${PATH_COHERENCE} -w /go/src/${PATH_COHERENCE} iron/go:dev go build -o coherence ${PATH_COHERENCE}/cmd/coherence
	docker build -t stickupkid/coherence:${TAG} .

.PHONY: push-docker-tag
push-docker-tag: FORCE
	@echo "Pushing '${TAG}' for '${BRANCH}'"
	docker login -u ${DOCKER_HUB_USERNAME} -p ${DOCKER_HUB_PASSWORD}
	docker push stickupkid/coherence:${TAG}

.PHONY: push-docker
ifeq ($(TAG),latest)
push-docker: FORCE
	@echo "Pushing '${TAG}' for '${BRANCH}'"
	docker login -u ${DOCKER_HUB_USERNAME} -p ${DOCKER_HUB_PASSWORD}
	docker push stickupkid/coherence:${TAG}
else
push-docker: FORCE
	@echo "Pushing requires branch '${BRANCH}' to be master"
endif
