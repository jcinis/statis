.PHONY: run image

IMAGE := jcinis/statis
VERSION := 1.0
MAKEPATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PWD := $(dir $(MAKEPATH))

image:
	docker build -f Dockerfile -t $(IMAGE):$(VERSION) ./
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest

run: image
	docker run \
		--rm \
		--name statis \
		-p 5000:5000 \
		-it $(IMAGE):$(VERSION)
