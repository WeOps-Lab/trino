# Makefile

# Variables
MVNW := ./mvnw
MVNW_FLAGS := -T8 clean install -DskipTests -Dair.check.skip-checkstyle=true
IMAGE_TAG := weops-influx
TRINO_REPO := docker-bkrepo.cwoa.net/ce1b09/weops-docker/trino

# Targets
build:
	$(MVNW) $(MVNW_FLAGS)
	cd ./core/docker && TRINO_VERSION=$(IMAGE_TAG) TRINO_REPO=$(TRNIO_REPO) ./build.sh -a amd64

push:
	docker push $(TRINO_REPO):$(IMAGE_TAG)
