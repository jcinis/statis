.PHONY: image shell test start-redis stop-redis

IMAGE := jcinis/statis
VERSION := 1.0
MAKEPATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PWD := $(dir $(MAKEPATH))

image:
	docker build -f Dockerfile -t $(IMAGE):$(VERSION) ./
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest

shell:
	make image
	make start-redis
	docker run \
		--rm \
		--name statis-shell \
		--link statis-redis:redis \
		-it $(IMAGE):$(VERSION) \
		/bin/sh
	make stop-redis

test:
	make image
	make start-redis
	docker run \
		--rm \
		--name statis-test \
		--link statis-redis:redis \
		-e "STATIS_REDIS_HOST=statis-redis" \
		-e "STATIS_REDIS_PORT=6379" \
		-it $(IMAGE):$(VERSION) \
		python test.py
	make stop-redis

start-redis:
	docker run \
		--rm \
		-d \
		--name statis-redis \
		-p 6379:6379 \
		-it redis \
		redis-server --appendonly yes

stop-redis:
	docker stop statis-redis

