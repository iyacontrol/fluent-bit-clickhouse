FROM fluent/fluent-bit:1.3.2
USER root
COPY out_clickhouse.so /fluent-bit/
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/fluent-bit/out_clickhouse.so"]