package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
	"github.com/netrebel/kafka-with-go/protos"
	"github.com/riferrei/srclient"
	proto "google.golang.org/protobuf/proto"
)

func createMessage(w http.ResponseWriter, r *http.Request) {
	// read body
	data, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	log.Printf("INFO: JSON Request: %v", string(data))

	// unmarshal and create docMsg
	msg := &protos.Life360AccountDeleted{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		log.Printf("ERROR: fail unmarshl: %s", err)
		response(w, "Invalid request json", 400)
	}

	log.Printf("INFO: Life360AccountDeleted: %v", msg)

	protoMsg, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalln("Failed to proto encode doc:", err)
	}

	err = PushMessageToTopic("life360_account_deleted", protoMsg)
	if err != nil {
		http.Error(w, "Error pushing to topic", http.StatusInternalServerError)
	}

}

func response(w http.ResponseWriter, resp string, status int) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, resp)
}

// PushMessageToTopic pushes commen to the topic
func PushMessageToTopic(topic string, message []byte) error {
	brokersURL := "localhost:32092"
	fmt.Printf("Connecting to Kafka on: %v\n", brokersURL)
	p, err := ConnectProducer(brokersURL)
	if err != nil {
		fmt.Printf("Error connecting to producer: %v", err)
		return err
	}
	defer p.Close()

	// 2) Fetch the latest version of the schema, or create a new one if it is the first
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:31081")
	schema, err := schemaRegistryClient.GetLatestSchema("life360_account_deleted-value_v1")
	if err != nil {
		panic(err)
	}
	if schema == nil {
		fmt.Println("Schema not found creating it")
		schemaBytes, err := ioutil.ReadFile("life360_account_deleted_v1.proto")
		if err != nil {
			panic(fmt.Sprintf("File not found %s", err))
		}

		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Protobuf)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 3) Serialize the record using the schema provided by the client,
	// making sure to include the schema id as part of the record.
	// newComplexType := ComplexType{ID: 1, Name: "Gopher"}
	// value, _ := json.Marshal(newComplexType)
	// native, _, _ := schema.Codec().NativeFromTextual(value)
	// valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, message...)

	delivery_chan := make(chan kafka.Event, 10000)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          recordValue},
		delivery_chan,
	)
	if err != nil {
		panic("Could not produce message")
	}
	fmt.Printf("Published message to topic %v\n", topic)
	p.Flush(1000)
	return nil
}

// ConnectProducer connects to Kafka
func ConnectProducer(brokersURL string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokersURL,
		"client.id":         "local",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	return p, nil
}

func main() {

	// router
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/account-delete", createMessage).Methods("POST")

	log.Printf("Start sending messages to localhost:3000/api/v1/account-delete")

	// start server
	err := http.ListenAndServe(":3000", r)
	if err != nil {
		fmt.Printf("ERROR: fail init http server, %s", err)
		os.Exit(1)
	}

}
