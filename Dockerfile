FROM golang:1.27-alpine3.24 AS source
ARG ver
WORKDIR /source
# Download modules in their own layer, so code changes don't invalidate it
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary, without local paths baked in
RUN CGO_ENABLED=0 go build -trimpath -o ./bin/hb -ldflags "-X main.version=${ver}" .

FROM alpine:3.24 AS app
# Fixed IDs, so a host directory mounted as the data dir can be chowned to match
RUN addgroup -S -g 10001 hb && adduser -S -D -H -u 10001 -G hb hb \
    && mkdir -p /var/hb/data && chown hb:hb /var/hb/data
WORKDIR /app
COPY --from=source /source/bin/hb ./hb
ENV HB_PORT=18950
ENV HB_DATA_DIR="/var/hb/data"
ENV HB_DB_IN_MEM="true"
EXPOSE ${HB_PORT}
USER hb
CMD ["/app/hb"]
