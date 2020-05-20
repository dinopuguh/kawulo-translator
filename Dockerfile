FROM golang:1.14.1

RUN mkdir -p /go/src/github.com/dinopuguh/kawulo-translator/

WORKDIR /go/src/github.com/dinopuguh/kawulo-translator/

COPY . .

RUN go build -o translator main.go

EXPOSE 9090

CMD /go/src/github.com/dinopuguh/kawulo-translator/translator