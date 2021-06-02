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
        private  IProducer<string, EmployeeEvent> _producer;
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

        public  async void ProduceRecord(string topicName, string key, EmployeeEvent value)
        {
            initProducer();
            var Message = new Message<string, EmployeeEvent> { Key = key, Value = value };
            

            DeliveryResult<string, EmployeeEvent> SentStatus = await _producer.ProduceAsync(topicName, Message);

            _logger.LogInformation("Produced message to topic '{Topic}', partition  '{TopicPartition}' , Offset '{TopicPartitionOffset}'",
                        SentStatus.Topic, SentStatus.TopicPartition.Partition.Value, SentStatus.TopicPartitionOffset.Offset);
            
        }

        private  void initProducer()
        {
            if (_producer == null)
            {
                var topicName = _configuration["ConfigProperties:Kafka:TopicName"];
                //output all the properties
                _logger.LogInformation("topic name {topic}", topicName);
                var Config =  _configUtil.LoadConfig();
                var producerConfig = new ProducerConfig(Config);
                //producerConfig.Partitioner = Partitioner.Murmur2Random;
                // custom partitioner needs to set on app that produces response
                _producer = new ProducerBuilder<string, EmployeeEvent>(Config)
                    .SetPartitioner(topicName, new PartitionerDelegate(_customPartitioner.customPartitioner))
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(SchemaRegistryUtil.GetSerializer())
                    .Build();

                _logger.LogInformation("Successfully constructed kafka producer");
            }
        }
    }
}
