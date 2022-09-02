package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func GetClient() *azservicebus.Client {
	connectionString, ok := os.LookupEnv("AZURE_SERVICEBUS_CONNECTION_STRING") //ex: Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>
	if !ok {
		panic("AZURE_SERVICEBUS_CONNECTION_STRING environment variable not found")
	}

	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func GetMessage(count int, client *azservicebus.Client) {
	queue, ok := os.LookupEnv("AZURE_SERVICEBUS_QUEUE_NAME") //ex: myqueue
	if !ok {
		panic("AZURE_SERVICEBUS_QUEUE_NAME environment variable not found")
	}
	receiver, err := client.NewReceiverForQueue(queue, nil) 
	if err != nil {
		panic(err)
	}
	defer receiver.Close(context.TODO())

	messages, err := receiver.ReceiveMessages(context.TODO(), count, nil)
	if err != nil {
		panic(err)
	}

	for _, message := range messages {
		var body []byte = message.Body
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n", string(body))

		err = receiver.CompleteMessage(context.TODO(), message, nil)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	client := GetClient()
	batchSizeInt := 0
	
	batchSize, ok := os.LookupEnv("BATCH_SIZE") //ex: 10
	if !ok {
		batchSizeInt = math.MaxInt32
	}

	batchSizeInt, err := strconv.Atoi(batchSize)
	if err != nil {
		batchSizeInt = math.MaxInt32
	}

	fmt.Println("\nget all messages:")
	GetMessage(batchSizeInt, client)
}