FROM golang:1.9.2-alpine3.6

LABEL maintainer="support@inwecrypto.com"

COPY . /go/src/github.com/inwecrypto/indexer

RUN go install github.com/inwecrypto/indexer/cmd/neo && rm -rf /go/src

VOLUME ["/etc/inwecrypto/indexer/neo"]

WORKDIR /etc/inwecrypto/indexer/

CMD ["/go/bin/neo","--conf","/etc/inwecrypto/indexer/neo/neo.json"]