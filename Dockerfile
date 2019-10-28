FROM golang:1.12 AS build-env
ADD ./  /go/src/github.com/iyacontrol/fluent-bit-clickhouse
WORKDIR /go/src/github.com/iyacontrol/fluent-bit-clickhouse
RUN go build -buildmode=c-shared -o clickhouse.so .

FROM fluent/fluent-bit:1.2.2
COPY --from=build-env /go/src/github.com/iyacontrol/fluent-bit-clickhouse/clickhouse.so /fluent-bit/
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/fluent-bit/clickhouse.so"]