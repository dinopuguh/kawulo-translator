package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/dinopuguh/kawulo-translator/kafka"
	"github.com/dinopuguh/kawulo-translator/models"
	"github.com/dinopuguh/kawulo-translator/services"
	"github.com/sirupsen/logrus"
)

var (
	brokers     = fmt.Sprintf("%v:%v", os.Getenv("KAFKA_SERVER"), os.Getenv("KAFKA_PORT"))
	group       = os.Getenv("KAWULO_TRANSLATOR_CONSUMER")
	src_topics  = os.Getenv("KAWULO_REVIEW_TOPICS")
	dest_topics = os.Getenv("KAWULO_TRANSLATE_TOPICS")
	version     = os.Getenv("KAFKA_VERSION")
)

type KafkaConsumer struct {
	ready chan bool
}

func main() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logrus.Infof("Brokers: %v, Group: %v, Src Topics: %v, Dest Topics: %v, Version: %v", brokers, group, src_topics, dest_topics, version)

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		logrus.Panicf("Error parsing Kafka version: %v", err)
	}

	kafkaConfig := kafka.GetKafkaConsumerConfig("", "")
	kafkaConfig.Version = version

	consumer := KafkaConsumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, kafkaConfig)
	if err != nil {
		logrus.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(src_topics, ","), &consumer); err != nil {
				logrus.Panicf("Error from consumer: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	logrus.Infoln("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		logrus.Infoln("terminating: context cancelled")
	case <-sigterm:
		logrus.Infoln("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		logrus.Panicf("Error closing client: %v", err)
	}
}

func (consumer *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		logrus.Infof("[%v/%v]", message.Partition, message.Offset)

		reviewMsg := &models.ReviewMessage{}

		err := json.Unmarshal([]byte(message.Value), reviewMsg)
		if err != nil {
			logrus.Errorf("Unable to unmarshal kafka message: %v", err)
			return err
		}

		text := reviewMsg.Review.Text
		lang := reviewMsg.Review.Lang

		translatedText, err := services.Translate(lang, text)
		if err != nil {
			logrus.Errorf("Unable to translate review: %v", err)
			return err
		}

		msg := models.TranslatedMessage{
			Location:   reviewMsg.Location,
			Restaurant: reviewMsg.Restaurant,
			Review:     reviewMsg.Review,
			Translated: translatedText,
		}

		json_msg, err := json.Marshal(msg)
		if err != nil {
			logrus.Errorf("Unable to convert review to JSON: %v", err)
		}

		logrus.Printf("Result: %v", string(json_msg))
		err = publishTranslatedText(string(json_msg))
		if err == nil {
			session.MarkMessage(message, "")
		}
	}

	return nil
}

func publishTranslatedText(msg string) error {
	kafkaConfig := kafka.GetKafkaProducerConfig("", "")
	producers, err := sarama.NewSyncProducer(strings.Split(brokers, ","), kafkaConfig)
	if err != nil {
		logrus.Errorf("Unable to create kafka producer got error %v", err)
		return err
	}
	defer func() {
		if err := producers.Close(); err != nil {
			logrus.Errorf("Unable to stop kafka producer: %v", err)
			return
		}
	}()

	logrus.Infof("Success create kafka sync-producer")

	kafkaProducer := &kafka.KafkaProducer{
		Producer: producers,
	}

	err = kafkaProducer.SendMessage(dest_topics, msg)
	if err != nil {
		return err
	}

	return nil
}
