.PHONY: build build-for-docker clean dockerize gen-mocks test
OUTPUT = function-controller
OUTPUT_LINUX = $(OUTPUT)-linux
BUILD_FLAGS =

ifeq ($(OS),Windows_NT)
    detected_OS := Windows
else
    detected_OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')
endif

ifeq ($(detected_OS),Linux)
	BUILD_FLAGS += -ldflags "-linkmode external -extldflags -static"
endif


GO_SOURCES = $(shell find pkg cmd -type f -name '*.go')

build: $(OUTPUT)

build-for-docker: $(OUTPUT_LINUX)

test:
	go test -v `glide nv`

$(OUTPUT): $(GO_SOURCES) vendor
	go build cmd/function-controller.go

$(OUTPUT_LINUX): $(GO_SOURCES) vendor
	# This builds the executable from Go sources on *your* machine, targeting Linux OS
	# and linking everything statically, to minimize Docker image size
	# See e.g. https://blog.codeship.com/building-minimal-docker-containers-for-go-applications/ for details
	CGO_ENABLED=0 GOOS=linux go build $(BUILD_FLAGS) -v -a -installsuffix cgo -o $(OUTPUT_LINUX) cmd/function-controller.go

vendor: glide.lock
	glide install -v --force

glide.lock: glide.yaml
	glide up -v --force

gen-mocks:
	mockery -name 'TopicInformer|FunctionInformer' -dir vendor/github.com/projectriff/kubernetes-crds/pkg/client/informers/externalversions/projectriff/v1
	mockery -name 'SharedIndexInformer' -dir vendor/k8s.io/client-go/tools/cache
	mockery -name 'DeploymentInformer' -dir vendor/k8s.io/client-go/informers/extensions/v1beta1
	mockery -name 'LagTracker|Deployer' -dir pkg/controller

clean:
	rm -f $(OUTPUT)
	rm -f $(OUTPUT_LINUX)

dockerize: build-for-docker
	docker build . -t projectriff/function-controller:0.0.3-snapshot
