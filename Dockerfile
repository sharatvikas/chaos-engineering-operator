# syntax=docker/dockerfile:1
# Multi-stage build for the chaos-engineering-operator controller-manager.
FROM golang:1.26 AS build
WORKDIR /workspace

# Cache module downloads
COPY go.mod go.sum ./
RUN go mod download

# Build the manager binary (static, so it runs on distroless).
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /chaos-operator ./cmd/operator

# Minimal, non-root runtime image (matches the Deployment's runAsNonRoot).
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /chaos-operator /chaos-operator
USER 65532:65532
ENTRYPOINT ["/chaos-operator"]
