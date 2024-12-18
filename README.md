# kafka-with-go

https://medium.com/swlh/apache-kafka-with-golang-227f9f2eb818

## How to run

* `docker-compose up -d` to run kafka
* `make build` to build the producer and consumer
* `make producer` to run the producer (starts HTTP server)
* `protoc *.proto --go_out=producer` to compile protobuf file
* `curl --location --request POST 'localhost:3000/api/v1/account-delete' --header 'Content-Type: application/json' --data-raw '{ "userId":"2016fe16-4e40-4b3c-87a2-3675ff1f8d97", "reason":"deleted" }'` to send a message to the producer
* `make consumer` to run the consumer


## Protobuf setup

```
# install protoc
brew install protobuf

# install go protobuf 
go get google.golang.org/protobuf

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# complie protobuf
protoc *.proto --go_out=producer
```

## Dependencies
```
cd docker360
make system-start
make kafka-environment-start
```
## M1 port-forward

Shouldn't be needed anymore.

```
kubectl port-forward service/life360-kafka 32092:9092
kubectl port-forward service/life360-schema-registry 31081:8081
```
## Schema registry

Links:
- https://github.com/riferrei/srclient
- https://github.com/riferrei/srclient/blob/master/EXAMPLES_PROTOBUF.md
- https://github.com/confluentinc/demo-scene/blob/master/getting-started-with-ccloud-golang/ClientApp.go
`http://localhost:31081/subjects/life360_account_deleted/versions/1`