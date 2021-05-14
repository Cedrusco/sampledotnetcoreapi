using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using Microsoft.VisualBasic.CompilerServices;
using sampledotnetcoreapi.Kafka;
using System.Threading;
using System.Data.HashFunction.MurmurHash;

namespace sampledotnetcoreapi.producer
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly IConfigUtil _configUtil;
        //Producer is thread safe as per the confluent kafka team
        private  IProducer<string, string> _producer;
        private IMurmurHashUtil _murmurHashUtil;

        public  KafkaProducer(IConfiguration configuration, ILogger<KafkaProducer> logger,
                    IConfigUtil configUtil, IMurmurHashUtil murmurHashUtil)
        {
            this._configuration = configuration;
            this._logger = logger;
            this._configUtil = configUtil;
            this._producer = null;
            this._murmurHashUtil = murmurHashUtil;
        }

        ~KafkaProducer()
        {
            _producer.Flush();
        }

        public  async void ProduceRecord(string topicName, string key, string value)
        {
            var Message = new Message<string, string> { Key = key, Value = value };
            
            if (_producer == null)
            {
                var kafkaConfigFile = _configuration["ConfigProperties:Kafka:ConfigFile"];
                var certFilePath = _configuration["ConfigProperties:Kafka:CertFile"];
                //output all the properties
                _logger.LogInformation("microsoft loglevel {topic}", _configuration["Logging:LogLevel:Microsoft"]);
                _logger.LogInformation("topic name {topic}", _configuration["ConfigProperties:Kafka:TopicName"]);
                _logger.LogInformation("config file {conffile}", kafkaConfigFile);
                var Config = await _configUtil.LoadConfig(kafkaConfigFile, certFilePath);
                var producerConfig = new ProducerConfig(Config);
                //producerConfig.Partitioner = Partitioner.Murmur2Random;
                // custom partitioner needs to set on app that produces response
                _producer = new ProducerBuilder<string, string>(Config)
                    .SetPartitioner(topicName, (string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                    {
                        var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                        _logger.LogInformation("Key string in partitioner : {key}", keyString);
                        return _murmurHashUtil.MurmurHash(keyData.ToArray(), partitionCount);
                    })
                    .Build();
      
                _logger.LogInformation("Successfully constructed kafka producer");
            }

            DeliveryResult<string, string> SentStatus = await _producer.ProduceAsync(topicName, Message);

            _logger.LogInformation("Produced message to topic '{Topic}', partition  '{TopicPartition}' , Offset '{TopicPartitionOffset}'",
                        SentStatus.Topic, SentStatus.TopicPartition.Partition.Value, SentStatus.TopicPartitionOffset.Offset);

            /*
            _producer.Produce(TopicName, Message, (SentStatus) =>
            {
                if (SentStatus.Error.Code != ErrorCode.NoError)
                {
                    _logger.LogWarning("Failed to produce message '{Reason}'", SentStatus.Error.Reason);
                }
                else
                {
                    _logger.LogInformation("Produced message to topic '{Topic}', partition  '{TopicPartition}' , Offset '{TopicPartitionOffset}'",
                        SentStatus.Topic, SentStatus.TopicPartition, SentStatus.TopicPartitionOffset);
                }

            });
            */
            
        }
    }
}
