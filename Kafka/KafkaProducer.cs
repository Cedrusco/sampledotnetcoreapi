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
using com.bswift.model.events.employee;
using Confluent.SchemaRegistry.Serdes;

namespace sampledotnetcoreapi.producer
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly IConfigUtil _configUtil;
        //Producer is thread safe as per the confluent kafka team
        private  IProducer<string, EmployeeUpdateEvent> _producer;
        private ICustomPartitioner _customPartitioner;

        public  KafkaProducer(IConfiguration configuration, ILogger<KafkaProducer> logger,
                    IConfigUtil configUtil, ICustomPartitioner customPartitioner)
        {
            this._configuration = configuration;
            this._logger = logger;
            this._configUtil = configUtil;
            this._producer = null;
            this._customPartitioner = customPartitioner;
            initProducer();
            _logger.LogInformation("Constructor called");
        }

        ~KafkaProducer()
        {
            _producer.Flush();
        }

        public  async void ProduceRecord(string topicName, string key, EmployeeUpdateEvent value)
        {
            initProducer();
            var Message = new Message<string, EmployeeUpdateEvent> { Key = key, Value = value };
            

            DeliveryResult<string, EmployeeUpdateEvent> SentStatus = await _producer.ProduceAsync(topicName, Message);

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

        private async void initProducer()
        {
            if (_producer == null)
            {
                var kafkaConfigFile = _configuration["ConfigProperties:Kafka:ConfigFile"];
                var certFilePath = _configuration["ConfigProperties:Kafka:CertFile"];
                var topicName = _configuration["ConfigProperties:Kafka:TopicName"];
                //output all the properties
                _logger.LogInformation("microsoft loglevel {topic}", _configuration["Logging:LogLevel:Microsoft"]);
                _logger.LogInformation("topic name {topic}", topicName);
                _logger.LogInformation("config file {conffile}", kafkaConfigFile);
                var Config = await _configUtil.LoadConfig(kafkaConfigFile, certFilePath);
                var producerConfig = new ProducerConfig(Config);
                //producerConfig.Partitioner = Partitioner.Murmur2Random;
                // custom partitioner needs to set on app that produces response
                _producer = new ProducerBuilder<string, EmployeeUpdateEvent>(Config)
                    .SetPartitioner(topicName, new PartitionerDelegate(_customPartitioner.customPartitioner))
                    .SetValueSerializer(SchemaRegistryUtil.GetSerializer())
                    .Build();

                _logger.LogInformation("Successfully constructed kafka producer");
            }
        }
    }
}
