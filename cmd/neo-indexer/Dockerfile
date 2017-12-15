FROM golang:1.9.2-stretch

LABEL maintainer="support@inwecrypto.com"

COPY . /go/src/github.com/inwecrypto/neo-indexer

RUN go install github.com/inwecrypto/neo-indexer/cmd/neo-indexer && rm -rf /go/src

VOLUME ["/etc/inwecrypto/indexer/neo"]

WORKDIR /etc/inwecrypto/indexer/neo

CMD ["/go/bin/neo-indexer","--conf","/etc/inwecrypto/indexer/neo/neo.json"]