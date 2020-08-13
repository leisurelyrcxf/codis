# Stage 1
FROM golang:1.13

RUN apt-get update
RUN apt-get install -y autoconf

ENV GOPATH /gopath
ENV CODIS  ${GOPATH}/src/github.com/CodisLabs/codis
ENV PATH   ${GOPATH}/bin:${PATH}:${CODIS}/bin

COPY . ${CODIS}

RUN go get go.etcd.io/etcd/clientv3
RUN make -C ${CODIS} distclean
RUN make -C ${CODIS} codis-dashboard codis-proxy codis-admin codis-fe

# Stage 2
FROM debian:buster

ENV GOPATH /gopath
ENV CODIS  ${GOPATH}/src/github.com/CodisLabs/codis

COPY --from=0 ${CODIS}/bin /usr/bin

WORKDIR /
