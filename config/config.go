package config

import (
	"bytes"
	"embed"
	"fmt"
	"go-cqrs-chat-example/app"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type KafkaConfig struct {
	BootstrapServers    []string            `mapstructure:"bootstrapServers"`
	Topic               string              `mapstructure:"topic"`
	NumPartitions       int32               `mapstructure:"numPartitions"`
	ReplicationFactor   int16               `mapstructure:"replicationFactor"`
	Retention           string              `mapstructure:"retention"`
	ConsumerGroup       string              `mapstructure:"consumerGroup"`
	KafkaProducerConfig KafkaProducerConfig `mapstructure:"producer"`
	KafkaConsumerConfig KafkaConsumerConfig `mapstructure:"consumer"`
}

type KafkaProducerConfig struct {
	RetryMax      int           `mapstructure:"retryMax"`
	ReturnSuccess bool          `mapstructure:"returnSuccess"`
	RetryBackoff  time.Duration `mapstructure:"retryBackoff"`
	ClientId      string        `mapstructure:"clientId"`
}

type KafkaConsumerConfig struct {
	ReturnErrors         bool          `mapstructure:"returnErrors"`
	ClientId             string        `mapstructure:"clientId"`
	NackResendSleep      time.Duration `mapstructure:"nackResendSleep"`
	ReconnectRetrySleep  time.Duration `mapstructure:"reconnectRetrySleep"`
	OffsetCommitInterval time.Duration `mapstructure:"offsetCommitInterval"`
}

type OtlpConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type HttpServerConfig struct {
	Address        string        `mapstructure:"address"`
	ReadTimeout    time.Duration `mapstructure:"readTimeout"`
	WriteTimeout   time.Duration `mapstructure:"writeTimeout"`
	MaxHeaderBytes int           `mapstructure:"maxHeaderBytes"`
}

type MigrationConfig struct {
	MigrationTable    string        `mapstructure:"migrationTable"`
	StatementDuration time.Duration `mapstructure:"statementDuration"`
}

type PostgreSQLConfig struct {
	Url                string          `mapstructure:"url"`
	MaxOpenConnections int             `mapstructure:"maxOpenConnections"`
	MaxIdleConnections int             `mapstructure:"maxIdleConnections"`
	MaxLifetime        time.Duration   `mapstructure:"maxLifetime"`
	MigrationConfig    MigrationConfig `mapstructure:"migration"`
	PrettyLog          bool            `mapstructure:"prettyLog"`
	Dump               bool            `mapstructure:"dump"`
}

type CqrsConfig struct {
	SleepBeforeEvent                time.Duration `mapstructure:"sleepBeforeEvent"`
	CheckAreEventsProcessedInterval time.Duration `mapstructure:"checkAreEventsProcessedInterval"`
	Dump                            bool          `mapstructure:"dump"`
	PrettyLog                       bool          `mapstructure:"prettyLog"`
	ExportConfig                    ExportConfig  `mapstructure:"export"`
	ImportConfig                    ImportConfig  `mapstructure:"import"`
}

type RestClientConfig struct {
	MaxIdleConns       int           `mapstructure:"maxIdleConns"`
	IdleConnTimeout    time.Duration `mapstructure:"idleConnTimeout"`
	DisableCompression bool          `mapstructure:"disableCompression"`
	Dump               bool          `mapstructure:"dump"`
	PrettyLog          bool          `mapstructure:"prettyLog"`
}

type ImportConfig struct {
	File string `mapstructure:"file"`
}

type ExportConfig struct {
	File string `mapstructure:"file"`
}

type ChatUserViewConfig struct {
	MaxViewableParticipants int32 `mapstructure:"maxViewableParticipants"`
}

type ProjectionsConfig struct {
	ChatUserViewConfig ChatUserViewConfig `mapstructure:"chatUserView"`
}

type LoggerConfig struct {
	Level string `mapstructure:"level"`
	Json  bool   `mapstructure:"json"`
}

type AaaConfig struct {
	AaaUrlConfig AaaUrlConfig `mapstructure:"url"`
}

type AaaUrlConfig struct {
	Base     string `mapstructure:"base"`
	GetUsers string `mapstructure:"getUsers"`
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
	AllowedMediaUrls  string `mapstructure:"allowedMediaUrls"`  // comma-separated
	AllowedIframeUrls string `mapstructure:"allowedIframeUrls"` // comma-separated
	MaxMedias         int    `mapstructure:"maxMedias"`
}

type AppConfig struct {
	KafkaConfig       KafkaConfig       `mapstructure:"kafka"`
	OtlpConfig        OtlpConfig        `mapstructure:"otlp"`
	PostgreSQLConfig  PostgreSQLConfig  `mapstructure:"postgresql"`
	HttpServerConfig  HttpServerConfig  `mapstructure:"server"`
	CqrsConfig        CqrsConfig        `mapstructure:"cqrs"`
	RestClientConfig  RestClientConfig  `mapstructure:"http"`
	ProjectionsConfig ProjectionsConfig `mapstructure:"projections"`
	LoggerConfig      LoggerConfig      `mapstructure:"logger"`
	AaaConfig         AaaConfig         `mapstructure:"aaa"`
	MessageConfig     MessageConfig     `mapstructure:"message"`
	FrontendUrl       string            `mapstructure:"frontendUrl"`
}

//go:embed config
var configFs embed.FS

func CreateTypedConfig() (*AppConfig, error) {
	return createTypedConfig("config-dev.yml")
}

func CreateTestTypedConfig() (*AppConfig, error) {
	return createTypedConfig("config-test.yml")
}

func createTypedConfig(filename string) (*AppConfig, error) {
	conf := AppConfig{}
	viper.SetConfigType("yaml")

	if embedBytes, err := configFs.ReadFile("config/" + filename); err != nil {
		return nil, fmt.Errorf("Fatal error during reading embedded config file: %s \n", err)
	} else if err = viper.ReadConfig(bytes.NewBuffer(embedBytes)); err != nil {
		return nil, fmt.Errorf("Fatal error during viper reading embedded config file: %s \n", err)
	}

	viper.SetEnvPrefix(strings.ToUpper(app.TRACE_RESOURCE))
	viper.AutomaticEnv()
	err := viper.GetViper().Unmarshal(&conf)
	if err != nil {
		return nil, fmt.Errorf("config file loaded failed. %v\n", err)
	}

	return &conf, nil
}
