package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
)

const (
	// How long to wait for leader election to occur before retrying
	// (default 6seconds). Similar to the JVM's `retry.backoff.ms`.
	DefaultMetadataRetryBackoff = 6 *time.Second
	// The total number of times to retry sending a message (default 10).
	// Similar to the `message.send.max.retries` setting of the JVM producer.
	DefaultMaxProducerRetry = 10
	// How long to wait for the cluster to settle between retries
	// (default 6seconds). Similar to the `retry.backoff.ms` setting of the
	// JVM producer.
	DefaultProducerBackoff = 6 * time.Second
	// Default kafka compression to handle data transfer costs and better storage
	// on kafka side. We are enabling with SnappyCompression. Override with
	// others, if needed
	DefaultCompression = sarama.CompressionSnappy
	// The maximum number of messages the producer will send in a single
	// broker request.Set to 0 for unlimited. This will however have issues
	// in terms of recovering from cluster failure. Similar to
	// `queue.buffering.max.messages` in the JVM producer.
	DefaultMaxFlushMessages = 1000

	//KafkaVersion default. In case of upgrading kafka, this needs to be changed
	DefaultKafkaVersion = "2.3.0"
	)

var compressionCodecMap = map[string]sarama.CompressionCodec{
	"none": sarama.CompressionNone,
	"gzip": sarama.CompressionGZIP,
	"snappy": sarama.CompressionSnappy,
	"lz4": sarama.CompressionLZ4,
	"zstd" : sarama.CompressionZSTD,
}

var partitionerMap = map[string]sarama.PartitionerConstructor{
	"random" : sarama.NewRandomPartitioner,
	"roundrobin": sarama.NewRoundRobinPartitioner,
	"hash": sarama.NewHashPartitioner,
}

type ProducerConfig struct {
	RetryBackoff time.Duration
	Partitioner string
	MaxRetry int
	MaxMessages int
	CompresionEnabled bool
	CompressionType string
	Brokers []string
	EnableTLS bool
	UserCertificate string
	UserKey         string
	CACertificate   string
	KafkaVersion    string
}

type KafkaQueue struct {
	Producer sarama.AsyncProducer
}

func isIntFieldSet(v int) bool {
	return v != 0
}

func isTimeFieldSet(v time.Duration) bool {
	return v != 0
}

func newTLSConfig(userCert, userKey, caCert string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.X509KeyPair([]byte(userCert), []byte(userKey))
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	if len(caCert) <= 0 {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))
	tlsConfig.RootCAs = caCertPool
	tlsConfig.InsecureSkipVerify = true

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}


func getKafkaConfig(pConfig *ProducerConfig) (*sarama.Config, error){
	config := sarama.NewConfig()
	kafkaVersion := pConfig.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = DefaultKafkaVersion
	}
	version, err := sarama.ParseKafkaVersion(DefaultKafkaVersion)
	if err != nil {
		fmt.Printf("Incorrect kafka version, Provided: %s, Max Available: %s, err: %s",kafkaVersion, sarama.MaxVersion, err.Error())
		return nil, err
	}
	config.Version = version
	if isTimeFieldSet(pConfig.RetryBackoff){
		config.Metadata.Retry.Backoff = pConfig.RetryBackoff
		config.Producer.Retry.Backoff = pConfig.RetryBackoff
	} else {
		config.Metadata.Retry.Backoff = DefaultMetadataRetryBackoff
		config.Producer.Retry.Backoff = DefaultProducerBackoff
	}
	if isIntFieldSet(pConfig.MaxMessages) {
		config.Producer.Flush.MaxMessages = pConfig.MaxMessages
	} else {
		config.Producer.Flush.MaxMessages = DefaultMaxFlushMessages
	}
	if isIntFieldSet(pConfig.MaxRetry) {
		config.Producer.Retry.Max = pConfig.MaxRetry
	} else {
		config.Producer.Retry.Max = DefaultMaxProducerRetry
	}

	//Authentication + TLS
	if pConfig.EnableTLS {
		if pConfig.CACertificate != "" && pConfig.UserCertificate != "" && pConfig.UserKey != ""{
			tlsConfig, err := newTLSConfig(pConfig.UserCertificate, pConfig.UserKey, pConfig.CACertificate)
			if err != nil {
				fmt.Printf("Error constructing Kafka TLS Config:%v\n", err)
				return nil, err
			} else {
				config.Net.TLS.Enable = true
				config.Net.TLS.Config = tlsConfig
			}
		}  else{
			fmt.Printf("TLS Enabled but one of the required fields of cacert/usercert/userkey is empty. Avoiding TLS")
		}
	}

	// Compression
	if pConfig.CompresionEnabled && pConfig.CompressionType != "none"{
		if val, ok := compressionCodecMap[pConfig.CompressionType]; ok {
			config.Producer.Compression = val
		} else {
			fmt.Printf("Compression enabled with unknown compression type:[%v]. Defaulting to Snappy Compression\n")
		}
	}

	//Partitioning
	if val, ok := partitionerMap[pConfig.Partitioner]; ok {
		config.Producer.Partitioner = val
	} else {
		fmt.Printf("Partioning algorithm specified[%v] is not valid. Resorting to default")
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	}


	// Initialize other constants
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	//config.Metadata.Retry.Backoff = 6 * time.Second
	config.Producer.Partitioner = sarama.NewRandomPartitioner


	config.ChannelBufferSize = 256

	config.Producer.Retry.Max = 3
	//config.Producer.Retry.Backoff= 6*time.Second
	config.Producer.Flush.MaxMessages = 100
	config.ChannelBufferSize = 256
	config.Producer.Compression = sarama.CompressionSnappy

	return config, nil
}

//NewKafkaProducer Sends message(string) to a given topic asynchronously
func NewKafkaProducer(pConfig *ProducerConfig) (*KafkaQueue, error) {
	config, err := getKafkaConfig(pConfig)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(pConfig.Brokers, config)
	if err != nil {
		return nil, err
	}
	return &KafkaQueue{producer}, nil
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}

//SendMessage Sends message(string) to a given topic asynchronously
func (q KafkaQueue) SendMessage(message string, topicName string) error {
	q.Producer.Input() <- prepareMessage(topicName, message)
	return nil
}

func (q KafkaQueue) Disconnect() {
	q.Producer.AsyncClose()
}
