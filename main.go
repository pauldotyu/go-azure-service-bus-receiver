package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func GetClient() *azservicebus.Client {
	// Use the connection string if it is available, otherwise use Azure Identity
	connectionString, ok := os.LookupEnv("AZURE_SERVICEBUS_CONNECTIONSTRING") //ex: Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>

	if ok {
		client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			log.Fatalf("failed to create a Service Bus client: %v", err)
		} else {
			fmt.Println("Service Bus client created from connection string")
			return client
		}
	} else {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			log.Fatalf("failed to obtain a credential: %v", err)
		}
		sbHostname, ok := os.LookupEnv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE") //ex: <YOUR_NAMESPACE>.servicebus.windows.net
		if !ok {
			panic("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE environment variable not found")
		}
		client, err := azservicebus.NewClient(sbHostname, cred, nil)
		if err != nil {
			log.Fatalf("failed to create a Service Bus client: %v", err)
		} else {
			fmt.Println("Service Bus client created with Azure Identity")
			return client
		}
	}

	return nil
}

func GetMessage(count int, client *azservicebus.Client) {
	queue, ok := os.LookupEnv("AZURE_SERVICEBUS_QUEUENAME") //ex: myqueue
	if !ok {
		panic("AZURE_SERVICEBUS_QUEUENAME environment variable not found")
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

	fmt.Println("\nget " + strconv.Itoa(batchSizeInt) + " messages at a time:")

	// loop to get messages
	for {
		GetMessage(batchSizeInt, client)
	}
}
