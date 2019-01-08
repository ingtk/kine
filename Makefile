PACKAGES = $(shell go list ./... |grep -v vendor)
FILES = $(shell find **/*.go |grep -v vendor )
fmt:
	echo $(FILES)
	goreturns -w $(FILES)
	goreturns -w *.go
	go fmt $(PACKAGES)

compile: fmt
	@go test -c

pretest:
	docker-compose stop
	docker-compose rm -f
	docker-compose up -d

test: fmt
	go test -v $(PACKAGES)

ci:
	@go test -v $(PACKAGES)

cov:
	gocov test $(PACKAGES) | gocov-html > ./coverage.html
	open ./coverage.html

cover: fmt
	go test --cover $(PACKAGES)


installdeps:
	@glide --debug i
	@find ./vendor -name ".git" | xargs rm -rf

updatedeps:
	@glide --debug up
	@find ./vendor -name ".git" | xargs rm -rf

deps:
	go get -u github.com/mitchellh/gox
	go get -u golang.org/x/tools/cmd/cover
	go get -u golang.org/x/tools/cmd/vet
	go get -u github.com/axw/gocov/gocov
	go get -u gopkg.in/matm/v1/gocov-html
	go get -u github.com/Masterminds/glide

checkdebug:
	@find . -path "./vendor" -prune -o -name "*.go" | grep -v vendor | xargs grep -E -n -e "pp\.|fmt\."

build: compile
	# GOOS=linux GOARCH=amd64 go build -o ./caspase_linux_amd64 -v cmd/*.go
	GOOS=darwin GOARCH=amd64 go build -o ./caspase_darwin_amd64 -v cmd/*.go

dev: compile
	@AWS_DYNAMODB_ENDPOINT=http://localhost:18000 \
	REDIS_ADDR=redis://localhost:6379 \
	go run main.go -ozenv local

.PHONY: fmt compile pretest test ci cov cover updatedeps mock build dev
# vim:set ts=4 sw=4 sts=4 noexpandtab:
