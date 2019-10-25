FROM golang:1.12 AS build-env
ADD ./  /go/src/github.com/iyacontrol/fluent-bit-clickhouse
WORKDIR /go/src/github.com/iyacontrol/fluent-bit-clickhouse
RUN go build -buildmode=c-shared -o out_clickhouse.so .

FROM fluent/fluent-bit:1.3.2
USER root
COPY --from=build-env /go/src/github.com/iyacontrol/fluent-bit-clickhouse/out_clickhouse.so /fluent-bit/
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/fluent-bit/out_clickhouse.so"]