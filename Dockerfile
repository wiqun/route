FROM golang:alpine

WORKDIR /go/src/app
COPY . .

RUN go env -w GOPROXY=https://goproxy.cn,direct && go mod tidy && go install ./...

#客户端端口
EXPOSE 4000
#集群端口
EXPOSE 4001

CMD ["route"]
