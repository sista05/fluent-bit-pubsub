FROM golang:1.16

ENV GO111MODULE on

COPY . /go/pubsub/

WORKDIR /go/pubsub

RUN go build -buildmode=c-shared -o pubsub.so .
