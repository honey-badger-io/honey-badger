FROM golang:1.25-alpine3.22 AS source
ARG ver
WORKDIR /source
COPY . .
RUN apk add make
RUN go version
RUN go install
RUN go build -o ./bin/hb -ldflags "-X main.version=${ver}" .

FROM alpine:3.22 AS app
WORKDIR /app
COPY --from=source /source/bin/hb ./hb
EXPOSE 18950
CMD ["/app/hb"]
