FROM golang:1.21-alpine3.18 AS source
ARG ver
WORKDIR /source
COPY . .
RUN apk add make
RUN go version
RUN go install
RUN go build -o ./bin/hb -ldflags "-X main.version=${ver}" .

FROM alpine:3.18.3 as app
WORKDIR /app
COPY --from=source /source/bin/hb ./hb
EXPOSE 18950
CMD ["/app/hb"]
