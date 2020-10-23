.DEFAULT_GOAL := build-all

export GO15VENDOREXPERIMENT=1

build-all: codis-dashboard codis-proxy codis-admin codis-ha codis-fe clean-gotest

codis-deps:
	echo "GO_MAJOR_VERSION: $(GO_MAJOR_VERSION)"
	@if [ $(GO_MAJOR_VERSION) -ge $(MINIMUM_SUPPORTED_GO_MAJOR_VERSION) ]; then \
		echo "Use go env -w to set GONOSUMDB, GONOPROXY, GOPRIVATE"; \
		go env -w GONOSUMDB="git.garena.com"; \
		go env -w GONOPROXY="git.garena.com"; \
		go env -w GOPRIVATE="git.garena.com"; \
	else \
		echo "Use export env var to set GONOSUMDB, GONOPROXY, GOPRIVATE"; \
		export GO111MODULE=on; \
		export GONOSUMDB="git.garena.com"; \
		export GONOPROXY="git.garena.com"; \
		export GOPRIVATE="git.garena.com"; \
	fi
	go env
	env GO111MODULE=on go mod download
	env GO111MODULE=on go mod vendor

	@mkdir -p bin config && bash version
	@make --no-print-directory -C extern/github.com/spinlock/jemalloc-go/

codis-dashboard: codis-deps
	go build -i -o bin/codis-dashboard ./cmd/dashboard
	@./bin/codis-dashboard --default-config > config/dashboard.toml

codis-proxy: codis-deps
	go build -i -tags "cgo_jemalloc" -o bin/codis-proxy ./cmd/proxy
	@./bin/codis-proxy --default-config > config/proxy.toml

codis-admin: codis-deps
	go build -i -o bin/codis-admin ./cmd/admin

codis-ha: codis-deps
	go build -i -o bin/codis-ha ./cmd/ha

codis-fe: codis-deps
	go build -i -o bin/codis-fe ./cmd/fe
	@rm -rf bin/assets; cp -rf cmd/fe/assets bin/

codis-server:
	@mkdir -p bin
	@rm -f bin/codis-server*
	make -j4 -C extern/redis-3.2.11/
	@cp -f extern/redis-3.2.11/src/redis-server  bin/codis-server
	@cp -f extern/redis-3.2.11/src/redis-benchmark bin/
	@cp -f extern/redis-3.2.11/src/redis-cli bin/
	@cp -f extern/redis-3.2.11/src/redis-sentinel bin/
	@cp -f extern/redis-3.2.11/redis.conf config/
	@sed -e "s/^sentinel/# sentinel/g" extern/redis-3.2.11/sentinel.conf > config/sentinel.conf

clean-gotest:
	@rm -rf ./pkg/topom/gotest.tmp

clean: clean-gotest
	@rm -rf bin
	@rm -rf scripts/tmp

distclean: clean
	@make --no-print-directory --quiet -C extern/redis-3.2.11 distclean
	@make --no-print-directory --quiet -C extern/github.com/spinlock/jemalloc-go/ distclean

gotest: codis-deps
	go test ./cmd/... ./pkg/...

gobench: codis-deps
	go test -gcflags -l -bench=. -v ./pkg/...

docker:
	docker build --force-rm -t codis-image .

demo:
	pushd example && make
