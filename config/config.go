package config

import (
	"embed"
	"errors"
	"fmt"
	"github.com/traefik/paerser/env"
	"github.com/traefik/paerser/file"
	"github.com/traefik/paerser/flag"
	"go-cqrs-chat-example/app"
	"log/slog"
	"os"
	"strings"
	"time"
)

const configLongPrefix = "--config"
const configShortPrefix = "-c"

type KafkaConfig struct {
	BootstrapServers  []string
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
	Retention         string
	ConsumerGroup     string
	Producer          KafkaProducerConfig
	Consumer          KafkaConsumerConfig
}

type KafkaProducerConfig struct {
	RetryMax      int
	ReturnSuccess bool
	RetryBackoff  time.Duration
	ClientId      string
}

type KafkaConsumerConfig struct {
	ReturnErrors         bool
	ClientId             string
	NackResendSleep      time.Duration
	ReconnectRetrySleep  time.Duration
	OffsetCommitInterval time.Duration
}

type OtlpConfig struct {
	Endpoint string
}

type HttpServerConfig struct {
	Address        string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes int
	Dump           bool
	PrettyLog      bool
}

type MigrationConfig struct {
	MigrationTable    string
	StatementDuration time.Duration
}

type PostgreSQLConfig struct {
	Url                string
	MaxOpenConnections int
	MaxIdleConnections int
	MaxLifetime        time.Duration
	Migration          MigrationConfig
	PrettyLog          bool
	Dump               bool
}

type CommandsConfig struct {
	MaxParticipantsPerSingleCommand int32
}

type CqrsConfig struct {
	SleepBeforeEvent                time.Duration
	CheckAreEventsProcessedInterval time.Duration
	Dump                            bool
	PrettyLog                       bool
	Export                          ExportConfig
	Import                          ImportConfig
	Projections                     ProjectionsConfig
	Commands                        CommandsConfig
}

type RestClientConfig struct {
	MaxIdleConns       int
	IdleConnTimeout    time.Duration
	DisableCompression bool
	Dump               bool
	PrettyLog          bool
}

type ImportConfig struct {
	File string
}

type ExportConfig struct {
	File string
}

type ChatUserViewConfig struct {
	MaxViewableParticipants         int32
	LastMessageMaxTextDbPreviewSize int32
}

type BlogViewConfig struct {
	MaxTextPreviewSize int32
}

type ProjectionsConfig struct {
	ChatUserView ChatUserViewConfig
	BlogView     BlogViewConfig
}

type LoggerConfig struct {
	Level string
	Json  bool
}

type AaaConfig struct {
	Url AaaUrlConfig
}

type AaaUrlConfig struct {
	Base        string
	GetUsers    string
	SearchUsers string
}

func (lc *LoggerConfig) GetLevel() slog.Leveler {
	var lvl slog.Level
	err := lvl.UnmarshalText([]byte(lc.Level))
	if err != nil {
		panic(err)
	}
	return lvl
}

type MessageConfig struct {
	AllowedMediaUrls            string // comma-separated
	AllowedIframeUrls           string // comma-separated
	MaxMedias                   int
	MaxDisplayableReactionUsers int
	PreviewMaxTextSize          int
}

type ChatConfig struct {
	TetATet TetATetConfig
}

type TetATetConfig struct {
	CanResend bool
	CanReact  bool
}

type RabbitMQConfig struct {
	Url                             string
	Debug                           bool
	CheckAreEventsProcessedInterval time.Duration // for tests
	MaxWaitForEvents                time.Duration // for tests
	DumpTestAccumulator             bool          // for tests
}

type AppConfig struct {
	Kafka       KafkaConfig
	Otlp        OtlpConfig
	PostgreSQL  PostgreSQLConfig
	Server      HttpServerConfig
	Cqrs        CqrsConfig
	Http        RestClientConfig
	Logger      LoggerConfig
	Aaa         AaaConfig
	Message     MessageConfig
	Chat        ChatConfig
	FrontendUrl string
	RabbitMQ    RabbitMQConfig
}

//go:embed config
var configFs embed.FS

func CreateTypedConfig(args []string) (*AppConfig, error) {
	return createTypedConfig("config-dev.yml", args[:]...)
}

func CreateTestTypedConfig() (*AppConfig, error) {
	return createTypedConfig("config-test.yml")
}

func createTypedConfig(filename string, args ...string) (*AppConfig, error) {
	conf := AppConfig{}
	var err error

	var argsToReadConfig []string

	if len(args) > 0 && (strings.HasPrefix(args[0], configLongPrefix) || strings.HasPrefix(args[0], configShortPrefix)) {
		// load provided config
		stringWithConfig := args[0]
		var thePath = stringWithConfig
		thePath, _ = strings.CutPrefix(thePath, configLongPrefix)
		thePath, _ = strings.CutPrefix(thePath, configShortPrefix)

		if strings.HasPrefix(thePath, "=") {
			thePath, _ = strings.CutPrefix(thePath, "=")
			argsToReadConfig = args[1:]
		} else {
			if len(args) < 2 {
				return nil, fmt.Errorf("expected file argument")
			}
			thePath = args[1]
			argsToReadConfig = args[2:]
		}

		thePath = strings.TrimSpace(thePath)

		err = file.Decode(thePath, &conf)
		if err != nil {
			return nil, fmt.Errorf("config file loaded failed. %v\n", err)
		}

	} else {
		// load default embed config
		embedBytes, err := configFs.ReadFile("config/" + filename)
		if err != nil {
			return nil, fmt.Errorf("Fatal error during reading embedded config file: %s \n", err)
		}
		fileContentString := string(embedBytes)

		err = file.DecodeContent(fileContentString, ".yml", &conf)

		if err != nil {
			return nil, fmt.Errorf("config file loaded failed. %v\n", err)
		}

		argsToReadConfig = args
	}

	err = env.Decode(os.Environ(), strings.ToUpper(app.TRACE_RESOURCE)+"_", &conf)
	if err != nil {
		return nil, err
	}

	err = flag.Decode(argsToReadConfig, &conf)
	if err != nil {
		return nil, err
	}

	err = validate(&conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

func validate(conf *AppConfig) error {
	if conf == nil {
		return errors.New("nil config")
	}

	if conf.Cqrs.Projections.ChatUserView.MaxViewableParticipants < 2 {
		return fmt.Errorf("max viewable participants = %d < 2", conf.Cqrs.Projections.ChatUserView.MaxViewableParticipants)
	}

	if conf.Cqrs.Commands.MaxParticipantsPerSingleCommand == 0 {
		return errors.New("max participants = per comamnd cannot be 0")
	}

	return nil
}
