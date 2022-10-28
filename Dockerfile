FROM golang:1.18-alpine as builder

RUN apk add --no-cache gcc musl-dev linux-headers git g++

ADD . /prysm
RUN cd /prysm && go build -o ./cmd/beacon-chain/beacon-chain ./cmd/beacon-chain

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /prysm/cmd/beacon-chain/beacon-chain /usr/local/bin

ENTRYPOINT ["beacon-chain"]