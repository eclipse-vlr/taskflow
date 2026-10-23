FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/taskflow ./cmd/api

FROM build AS verify
RUN apk add --no-cache build-base
ENV CGO_ENABLED=1
COPY tests ./tests
COPY migrations ./migrations
CMD ["sh", "-c", "go test -race -count=1 -tags=integration ./... && go vet -tags=integration ./... && go mod verify"]

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -H taskflow
COPY --from=build /out/taskflow /usr/local/bin/taskflow
USER taskflow
EXPOSE 8080
ENTRYPOINT ["taskflow"]
