FROM golang:latest
ADD . /go/src/github.com/inwecrypto/indexer
RUN go install github.com/inwecrypto/indexer/cmd/neo && rm -rf /go/src

VOLUME [ "/etc/inwecrypto/indexer/neo" ]

ENTRYPOINT ["/go/bin/neo"]