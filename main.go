package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func main() {
	keypair, err := tls.LoadX509KeyPair("/Users/vishmeduri/Config/service.cert", "/Users/vishmeduri/Config/service.key")
	if err != nil {
		log.Println(err)
		return
	}

	caCert, err := ioutil.ReadFile("/Users/vishmeduri/Config/ca.pem")
	if err != nil {
		log.Println(err)
		return
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{keypair},
		RootCAs:      caCertPool,
	}

	// init config, enable errors and notifications
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Version = sarama.V0_10_2_0

	HOST := os.Getenv("HOST")
	SSL_PORT := os.Getenv("SSL_PORT")
	//handle error for missing env variables

	if HOST == "" || SSL_PORT == "" {
		log.Println("Missing env variables HOST or SSL_PORT for Aiven Kafka")
		log.Println("EXPORT HOST or SSL_PORT for Aiven Kafka")

		return

	}

	brokers := []string{HOST + ":" + SSL_PORT}

	log.Println(HOST + SSL_PORT)

	producer, err := sarama.NewSyncProducer(brokers, config)

	msg := &sarama.ProducerMessage{
		Topic: "test_auth_network",
		Value: sarama.StringEncoder("testing 123"),
	}

	//send message

	partition, offset, err := producer.SendMessage(msg)

	//log

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", msg.Topic, partition, offset)

	// add your logic
	if err != nil {
		log.Println(err)
		return
	}

	// close producer
	if err := producer.Close(); err != nil {
		log.Println(err)
		return
	}

}
