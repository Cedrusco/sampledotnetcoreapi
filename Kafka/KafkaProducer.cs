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

namespace sampledotnetcoreapi.producer
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly IConfigUtil _configUtil;
        //Producer is thread safe as per the confluent kafka team
        private  IProducer<string, string> _producer;

        public KafkaProducer(IConfiguration Configuration, ILogger<KafkaProducer> Logger,
                    IConfigUtil ConfigUtil)
        {
            this._configuration = Configuration;
            this._logger = Logger;
            this._configUtil = ConfigUtil;
            var KafkaConfigFile = Configuration["ConfigProperties:Kafka:ConfigFile"];
            var CertFilePath = Configuration["ConfigProperties:Kafka:CertFile"];
            //output all the properties
            _logger.LogInformation("microsoft loglevel {topic}", Configuration["Logging:LogLevel:Microsoft"]);
            _logger.LogInformation("topic name {topic}", Configuration["ConfigProperties:Kafka:TopicName"]);
            _logger.LogInformation("config file {conffile}", KafkaConfigFile);           
            var Config = _configUtil.LoadConfig(KafkaConfigFile, CertFilePath);
            _producer = new ProducerBuilder<string, string>(Config).Build();
          
            _logger.LogInformation("Successfully constructed kafka producer");
        }

        ~KafkaProducer()
        {
            _producer.Flush();
        }

        public  void ProduceRecord(string TopicName, string key, string value)
        {
            var Message = new Message<string, string> { Key = key, Value = value };
            /**
            CancellationToken cts = new CancellationToken();

            DeliveryResult<string, string> SentStatus = await _producer.ProduceAsync(TopicName, Message, cts);

            _logger.LogInformation("Produced message to topic '{Topic}', partition  '{TopicPartition}' , Offset '{TopicPartitionOffset}'",
                        SentStatus.Topic, SentStatus.TopicPartition, SentStatus.TopicPartitionOffset);
            **/
            
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
            
        }
    }
}
