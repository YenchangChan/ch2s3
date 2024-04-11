VERSION ?= "1.0.0"
TIMESTAMP = $(shell date +%Y%m%d)
TIME=$(shell date +%Y%m%dT%H:%M:%S)
REVISION=$(shell git log --oneline | head -n1 | cut -f1 -d" ")

.PHONY: pre
pre:
	go mod tidy

.PHONY: build
build: pre
	go build -ldflags "-X main.BuildStamp=${TIME} -X main.Githash=${REVISION} -X main.Version=${VERSION}"

.PHONY: package
package: build
	@rm -rf ./release
	@mkdir -p ./release/ch2s3/bin ./release/ch2s3/conf ./release/ch2s3/reporter ./release/ch2s3/docs
	@cp README.md ./release/ch2s3/docs
	@mv ch2s3 ./release/ch2s3/bin
	@cp resource/backup.json ./release/ch2s3/conf
	@cd release && tar -czf ../ch2s3-${VERSION}-${TIMESTAMP}-${REVISION}.tar.gz ch2s3
	@rm -rf ./release

