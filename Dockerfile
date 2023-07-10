FROM golang:1.20-alpine3.18 AS builder

ARG GITHUB_TOKEN=""

# Setup base software for building app
RUN apk update && \
    apk add bash ca-certificates git gcc g++ libc-dev binutils file

# Setup token to access private repositories in gitlab. Temp solution.
RUN git config --global --add \
    url."https://oauth2:${GITHUB_TOKEN}@github.com/goverland-labs/".insteadOf "https://github.com/goverland-labs/"

WORKDIR /opt

# Set up go env
RUN go env -w GOPRIVATE=github.com/goverland-labs/*

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy an application's source
COPY . .

# Build an application
RUN go build -o bin/application .

# Prepare executor image
FROM alpine:3.18 AS production

RUN apk update && \
    apk add ca-certificates libc6-compat && \
    rm -rf /var/cache/apk/*

WORKDIR /opt

COPY --from=builder /opt/bin/ ./

CMD ["./application"]
