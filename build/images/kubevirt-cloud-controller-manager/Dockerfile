FROM --platform=linux/amd64 docker.io/library/golang:1.25.5 AS builder

WORKDIR /go/src/kubevirt.io/cloud-provider-kubevirt

# Copy module files first for better layer caching
COPY go.mod go.sum ./

# Download dependencies (will be cached if go.mod/go.sum unchanged)
RUN go mod download

# Copy source code
COPY pkg/ pkg/
COPY cmd/ cmd/

# Build without -mod=vendor (default behavior uses modules + downloaded deps)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/kubevirt-cloud-controller-manager ./cmd/kubevirt-cloud-controller-manager

FROM registry.access.redhat.com/ubi9/ubi-micro

COPY --from=builder /go/src/kubevirt.io/cloud-provider-kubevirt/bin/kubevirt-cloud-controller-manager /bin/kubevirt-cloud-controller-manager

ENTRYPOINT [ "/bin/kubevirt-cloud-controller-manager" ]