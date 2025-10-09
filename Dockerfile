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
ENV HB_PORT=18950
ENV HB_DATA_DIR="/var/hb/data"
ENV HB_DB_IN_MEM="true"
EXPOSE ${HB_PORT}
CMD ["/app/hb"]
