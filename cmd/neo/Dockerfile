FROM golang:latest
ADD . /go/src/github.com/inwecrypto/indexer
RUN go install github.com/inwecrypto/indexer/cmd/neo

VOLUME [ "/etc/inwecrypto/indexer/neo" ]

ENTRYPOINT ["/go/bin/neo"]