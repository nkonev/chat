package kafka

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"io"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/Jeffail/gabs/v2"
	"go.uber.org/fx"
)

const kafkaConfigRetentionMs = "retention.ms"

func ConfigureKafkaAdmin(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (sarama.ClusterAdmin, error) {
	kafkaAdminConfig := sarama.NewConfig()
	kafkaAdminConfig.Version = sarama.V4_1_0_0

	kafkaAdmin, err := sarama.NewClusterAdmin(cfg.Kafka.BootstrapServers, kafkaAdminConfig)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Stopping kafka admin")

			if err := kafkaAdmin.Close(); err != nil {
				lgr.Error("Error shutting down kafka admin", logger.AttributeError, err)
			}
			return nil
		},
	})

	return kafkaAdmin, nil
}

func RunCreateTopicChat(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	retention := cfg.Kafka.TopicChat.Retention
	topicName := cfg.Kafka.TopicChat.Topic
	lgr.Info("Creating topic", "topic", topicName)

	err := kafkaAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     cfg.Kafka.TopicChat.NumPartitions,
		ReplicationFactor: cfg.Kafka.TopicChat.ReplicationFactor,
		ConfigEntries: map[string]*string{
			// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
			kafkaConfigRetentionMs: &retention,
		},
	}, false)
	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
		lgr.Info("Topic is already exists", "topic", topicName)
	} else if err != nil {
		return err
	} else {
		lgr.Info("Topic was successfully created", "topic", topicName)
	}

	return nil
}

func RunCreateTopicUser(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	retention := cfg.Kafka.TopicUser.Retention
	topicName := cfg.Kafka.TopicUser.Topic
	lgr.Info("Creating topic", "topic", topicName)

	err := kafkaAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     cfg.Kafka.TopicUser.NumPartitions,
		ReplicationFactor: cfg.Kafka.TopicUser.ReplicationFactor,
		ConfigEntries: map[string]*string{
			// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
			kafkaConfigRetentionMs: &retention,
		},
	}, false)
	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
		lgr.Info("Topic is already exists", "topic", topicName)
	} else if err != nil {
		return err
	} else {
		lgr.Info("Topic was successfully created", "topic", topicName)
	}

	return nil
}

func RunDeleteTopicChat(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	lgr.Warn("Removing topic", "topic", cfg.Kafka.TopicChat.Topic)
	err := kafkaAdmin.DeleteTopic(cfg.Kafka.TopicChat.Topic)
	if err != nil {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			lgr.Warn("Topic does not exists", "topic", cfg.Kafka.TopicChat.Topic)
		} else {
			return err
		}
	}
	lgr.Warn("Topic was removed", "topic", cfg.Kafka.TopicChat.Topic)
	return nil
}

func RunDeleteTopicUser(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	lgr.Warn("Removing topic", "topic", cfg.Kafka.TopicUser.Topic)
	err := kafkaAdmin.DeleteTopic(cfg.Kafka.TopicUser.Topic)
	if err != nil {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			lgr.Warn("Topic does not exists", "topic", cfg.Kafka.TopicUser.Topic)
		} else {
			return err
		}
	}
	lgr.Warn("Topic was removed", "topic", cfg.Kafka.TopicUser.Topic)
	return nil
}

func RunResetPartitionsChat(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	lgr.Info("Start reset partitions")

	err := kafkaAdmin.DeleteConsumerGroup(cfg.Kafka.ConsumerGroupChat)

	if err != nil {
		if errors.Is(err, sarama.ErrGroupIDNotFound) {
			lgr.Info("There is no consumer group", "consumer_group", cfg.Kafka.ConsumerGroupChat)
		} else {
			return err
		}
	}

	lgr.Info("Finished reset partitions")

	return nil
}

func RunResetPartitionsUser(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	lgr.Info("Start reset partitions")

	err := kafkaAdmin.DeleteConsumerGroup(cfg.Kafka.ConsumerGroupUser)

	if err != nil {
		if errors.Is(err, sarama.ErrGroupIDNotFound) {
			lgr.Info("There is no consumer group", "consumer_group", cfg.Kafka.ConsumerGroupUser)
		} else {
			return err
		}
	}

	lgr.Info("Finished reset partitions")

	return nil
}

func ConfigureSaramaClient(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0

	client, err := sarama.NewClient(cfg.Kafka.BootstrapServers, config)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx0 context.Context) error {
			lgr.Info("Stopping kafka client")
			ce := client.Close()
			lgr.Info("Kafka client stopped", logger.AttributeError, ce)

			return nil
		},
	})

	return client, nil
}

type topicKind int16

const (
	topicKindUnspecified = iota
	topicKindChat
	topicKindUser
)

func (t topicKind) String() string {
	switch t {
	case topicKindUnspecified:
		return "unspecified"
	case topicKindChat:
		return "chat"
	case topicKindUser:
		return "user"
	}
	return "unknown"
}

func WaitForAllEventsProcessedChat(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) error {
	return waitForAllEventsProcessed(lgr, cfg, saramaClient, lc, topicKindChat)
}

func WaitForAllEventsProcessedUser(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) error {
	return waitForAllEventsProcessed(lgr, cfg, saramaClient, lc, topicKindUser)
}

// https://github.com/IBM/sarama/wiki/Frequently-Asked-Questions#how-do-i-consume-until-the-end-of-a-partition
func waitForAllEventsProcessed(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
	topicKind topicKind,
) error {
	stoppingCtx, cancelFunc := context.WithCancel(context.Background())

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Stopping waiter")
			cancelFunc()
			return nil
		},
	})

	du := cfg.Cqrs.CheckAreEventsProcessedInterval

	for {
		lgr.Info("Checking for the current offsets will be equal to the latest ones for all partitions", "topic_kind", topicKind.String())
		isEnd, errE := isEndOnAllPartitions(lgr, cfg, saramaClient, topicKind)
		if errE != nil {
			lgr.Error("Error during checking isEndOnAllPartitions", logger.AttributeError, errE)
			return errE
		}
		if isEnd {
			lgr.Info("All the events was processed", "topic_kind", topicKind.String())
			cancelFunc()
		} else {
			lgr.Info("The current offsets still aren't equal to the latest ones")
		}

		if errors.Is(stoppingCtx.Err(), context.Canceled) {
			lgr.Info("Exiting from waiter", "topic_kind", topicKind.String())
			break
		} else {
			lgr.Info("Will wait before the next check iteration", "duration", du, "topic_kind", topicKind.String())
			time.Sleep(du)
		}
	}

	return nil
}

func getMaxOffsets(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	client sarama.Client,
	topicKind topicKind,
) ([]int64, error) {
	var ktc config.KafkaTopicConfig
	switch topicKind {
	case topicKindChat:
		ktc = cfg.Kafka.TopicChat
	case topicKindUser:
		ktc = cfg.Kafka.TopicUser
	default:
		return nil, fmt.Errorf("Unknown topicKind: %v", topicKind)
	}

	maxOffsets := make([]int64, ktc.NumPartitions)

	for i := range ktc.NumPartitions {
		offset, err := client.GetOffset(ktc.Topic, i, sarama.OffsetNewest)
		if err != nil {
			return maxOffsets, err
		}
		maxOffsets[i] = offset
		lgr.Debug("Got max", "partition", i, "offset", offset)
	}
	return maxOffsets, nil
}

func isEndOnAllPartitions(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	client sarama.Client,
	topicKind topicKind,
) (bool, error) {

	maxOffsets, err := getMaxOffsets(lgr, cfg, client, topicKind)
	if err != nil {
		if errors.Is(err, sarama.ErrNotLeaderForPartition) {
			return false, nil
		}
		return false, err
	}

	// check are all 0
	allZero := true
	for p := range maxOffsets {
		if maxOffsets[p] != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return true, nil
	}

	var consumerGroup string
	var ktc config.KafkaTopicConfig
	switch topicKind {
	case topicKindChat:
		consumerGroup = cfg.Kafka.ConsumerGroupChat
		ktc = cfg.Kafka.TopicChat
	case topicKindUser:
		consumerGroup = cfg.Kafka.ConsumerGroupUser
		ktc = cfg.Kafka.TopicUser
	default:
		return false, fmt.Errorf("Unknown topicKind: %v", topicKind)
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroup, client)
	if err != nil {
		return false, err
	}
	defer offsetManager.Close()

	givenOffsets := make([]int64, ktc.NumPartitions)
	for i := range ktc.NumPartitions {
		partitionManager, err := offsetManager.ManagePartition(ktc.Topic, i)
		if err != nil {
			if errors.Is(err, sarama.ErrIncompleteResponse) {
				lgr.Info("Skipping partition", "partition", i)
				return false, nil
			}
			return false, err
		}
		defer partitionManager.AsyncClose() // faster

		offs, _ := partitionManager.NextOffset()
		if err != nil {
			return false, err
		}
		givenOffsets[i] = offs
		lgr.Debug("Got given", "partition", i, "offset", offs)
	}

	hasOneInitialized := false
	for i := range ktc.NumPartitions {
		if givenOffsets[i] == -1 {
			continue
		} else {
			hasOneInitialized = true

			if maxOffsets[i] != givenOffsets[i] {
				return false, nil
			}
		}
	}

	return hasOneInitialized, nil
}

const KeyKey = "key"
const ValueKey = "value"
const MetadataKey = "metadata"
const MetadataOffsetKey = "offset"
const MetadataPartitionKey = "partition"
const HeadersKey = "headers"

func Export(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
) error {

	maxOffsets, err := getMaxOffsets(lgr, cfg, saramaClient, topicKindChat)
	if err != nil {
		return err
	}

	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0

	newConsumer, err := sarama.NewConsumer(cfg.Kafka.BootstrapServers, config)
	if err != nil {
		return err
	}
	defer newConsumer.Close()

	var writer io.Writer
	var f *os.File
	if cfg.Cqrs.Export.File == app.PseudoFileStdout {
		writer = os.Stdout
	} else {
		f, err = os.Create(cfg.Cqrs.Export.File)
		if err != nil {
			return err
		}
		writer = f
	}
	if f != nil {
		defer f.Close()
	}

	for i := range cfg.Kafka.TopicChat.NumPartitions {
		partitionMaxOffset := maxOffsets[i]
		if partitionMaxOffset == 0 {
			lgr.Info("Skipping partition because absence of messages", "partition", i)
			continue
		}

		lgr.Info("Reading partition and it's max offset", "partition", i, "offset", partitionMaxOffset)

		partitionConsumer, err := newConsumer.ConsumePartition(cfg.Kafka.TopicChat.Topic, i, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		defer partitionConsumer.Close()

		for kafkaMessage := range partitionConsumer.Messages() {
			jsonObj := gabs.New()
			_, err = jsonObj.SetP(kafkaMessage.Offset, MetadataKey+"."+MetadataOffsetKey)
			if err != nil {
				return err
			}
			_, err = jsonObj.SetP(kafkaMessage.Partition, MetadataKey+"."+MetadataPartitionKey)
			if err != nil {
				return err
			}

			parsedKey := string(kafkaMessage.Key)
			parsedValue, err := gabs.ParseJSON(kafkaMessage.Value)
			if err != nil {
				return err
			}

			for _, h := range kafkaMessage.Headers {
				parsedHeaderKey := string(h.Key)
				parsedHeaderValue := string(h.Value)

				_, err = jsonObj.Set(parsedHeaderValue, HeadersKey, parsedHeaderKey)
				if err != nil {
					return err
				}
			}

			_, err = jsonObj.Set(parsedKey, KeyKey)
			if err != nil {
				return err
			}

			_, err = jsonObj.Set(parsedValue, ValueKey)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(writer, jsonObj.String())
			if err != nil {
				return err
			}

			if kafkaMessage.Offset >= partitionMaxOffset-1 {
				lgr.Info("Reached max offset, closing partitionConsumer", "partition", i)
				break
			}
		}

		lgr.Info("Finish reading partition", "partition", i)
	}
	return nil
}

func Import(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
) error {
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.Kafka.BootstrapServers, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	var reader io.Reader
	var f *os.File
	if cfg.Cqrs.Import.File == app.PseudoFileStdin {
		reader = os.Stdin
	} else {
		f, err = os.Open(cfg.Cqrs.Import.File)
		if err != nil {
			return err
		}
		reader = f
	}
	if f != nil {
		defer f.Close()
	}

	scanner := bufio.NewScanner(reader)
	i := 0
	for scanner.Scan() {
		i++
		str := scanner.Text()
		jsonObj, err := gabs.ParseJSON([]byte(str))
		if err != nil {
			return fmt.Errorf("Error on reading line %v: %w", i, err)
		}

		kd := jsonObj.S(KeyKey).Data()
		aKey, okk := kd.(string)
		if !okk {
			return fmt.Errorf("Error on parsing key on reading line %v from %v", i, kd)
		}

		aValue := jsonObj.S(ValueKey).Bytes()
		aPartition := jsonObj.S(MetadataKey, MetadataPartitionKey).String()
		partition, err := utils.ParseInt64(aPartition)
		if err != nil {
			return fmt.Errorf("Error on parsing partition on reading line %v: %w", i, err)
		}

		msg := &sarama.ProducerMessage{
			Topic:     cfg.Kafka.TopicChat.Topic,
			Key:       sarama.ByteEncoder(aKey),
			Value:     sarama.ByteEncoder(aValue),
			Partition: int32(partition),
		}

		for headerKey, headerValue := range jsonObj.S(HeadersKey).ChildrenMap() {
			hd := headerValue.Data()
			hds, okhv := hd.(string)
			if !okhv {
				return fmt.Errorf("Error on parsing header value on reading line %v from %v for key %v", i, hd, headerKey)
			}
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(headerKey),
				Value: []byte(hds),
			})
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("Error on sending message from line %v: %w", i, err)
		}
	}

	lgr.Info("Import was successfully finished")
	return nil
}
