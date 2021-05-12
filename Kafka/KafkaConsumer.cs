using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private IConsumer<string, string> _kafkaConsumer;
        private readonly IConfigUtil _configUtil;
        private readonly ISynchronzationUtil _synchronzationUtil;

        private readonly string _topicName;
        private ConcurrentDictionary<string, string> _responseMap = new ConcurrentDictionary<string, string>();

        public KafkaConsumer(ILogger<KafkaConsumer> logger, 
                                IConfiguration configuration,
                                   IConfigUtil configUtil,
                                   ISynchronzationUtil synchronzationUtil)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._configUtil = configUtil;
            this._synchronzationUtil = synchronzationUtil;
            _topicName = _configuration["ConfigProperties:Kafka:ResponseTopicName"];
        }
        public string GetResponseById(string requestId)
        {
            string responseValue = null;
            if (!_responseMap.TryGetValue(requestId, out responseValue))
            {
                _logger.LogError("Error getting the response value for requestId from response map requestId={requestId}", requestId);
            }
            return responseValue;
        }

        public bool IsAssignmentPartition(int partitionId)
        {
            EnsureConsumer();
            foreach( var Assignment in _kafkaConsumer.Assignment)
            {
                if (Assignment.Partition.Equals(partitionId) && 
                        Assignment.Topic.Equals(_topicName))
                {
                    return true;
                }
            }

            return false;
        }

        public void StartConsumer()
        {

            EnsureConsumer();
            //ConsumerRebalanceListener not used for now
            _kafkaConsumer.Subscribe(_topicName);
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    var ConsumerRecord = _kafkaConsumer.Consume(cts.Token);
                    _logger.LogInformation("Consumed the record key = {Key}, value = {Value}",
                        ConsumerRecord.Message.Key, ConsumerRecord.Message.Value) ;
                    // Get the lock object for request
                    EventWaitHandle lockObject = _synchronzationUtil.GetLockObject(ConsumerRecord.Message.Key);
                    if (lockObject != null)
                    {
                        if (!_responseMap.TryAdd(ConsumerRecord.Message.Key, ConsumerRecord.Message.Value))
                        {
                            _logger.LogWarning("Error adding response to response map for request {Key}", ConsumerRecord.Message.Key);
                        }
                        lockObject.Set(); // Signal the main controller thread so that it can get the response
                    }                  
                }
            }
            catch (OperationCanceledException)
            {
                //commit offset manually
                try
                {
                    _kafkaConsumer.Commit();
                }
                catch (KafkaException e)
                {
                    _logger.LogWarning("Exceptiopn while committing the offsets on ctrl-c {message}", e.Message);
                }
            }
            finally
            {
                _kafkaConsumer.Close();
            }
        }

        private async void EnsureConsumer()
        {
            if (_kafkaConsumer == null)
            {
                var kafkaConfigFile = _configuration["ConfigProperties:Kafka:ConfigFile"];
                var certFilePath = _configuration["ConfigProperties:Kafka:CertFile"];
                var config = await _configUtil.LoadConfig(kafkaConfigFile, certFilePath);
                var consumerConfig = new ConsumerConfig(config);
                // INSTANCEID can be omitted depending upon how we choose requestId
                consumerConfig.GroupId = _configuration["ConfigProperties:Kafka:ConsumerGroupId"] + Environment.GetEnvironmentVariable("INSTANCE_ID");
                consumerConfig.AutoOffsetReset = (AutoOffsetReset)Enum.Parse(typeof(AutoOffsetReset), _configuration["ConfigProperties:Kafka:AutoOffsetReset"]);
                consumerConfig.EnableAutoCommit = true;
                _kafkaConsumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
                _logger.LogInformation("Successfully constructed KafkaConsumer");
            }
        }
    }
}
