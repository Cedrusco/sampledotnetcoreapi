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

        public KafkaConsumer(ILogger<KafkaConsumer> Logger, 
                                IConfiguration Configuration,
                                   IConfigUtil ConfigUtil,
                                   ISynchronzationUtil SynchronzationUtil)
        {
            this._logger = Logger;
            this._configuration = Configuration;
            this._configUtil = ConfigUtil;
            this._synchronzationUtil = SynchronzationUtil;
            _topicName = _configuration["ConfigProperties:Kafka:ResponseTopicName"];
        }
        public string getResponseById(string requestId)
        {
            string responseValue = null;
            if (!_responseMap.TryGetValue(requestId, out responseValue))
            {
                _logger.LogError("Error getting the response value for requestId from response map requestId={requestId}", requestId);
            }
            return responseValue;
        }

        public bool isAssignmentPartition(int partitionId)
        {
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

        public async void startConsumer()
        {

            if (_kafkaConsumer == null)
            {
                var KafkaConfigFile = _configuration["ConfigProperties:Kafka:ConfigFile"];
                var CertFilePath = _configuration["ConfigProperties:Kafka:CertFile"];
                var Config = await _configUtil.LoadConfig(KafkaConfigFile, CertFilePath);
                var ConsumerConfig = new ConsumerConfig(Config);
                // INSTANCEID can be omitted depending upon how we choose requestId
                ConsumerConfig.GroupId = _configuration["ConfigProperties:Kafka:ConsumerGroupId"] + Environment.GetEnvironmentVariable("INSTANCE_ID");
                ConsumerConfig.AutoOffsetReset = (AutoOffsetReset)Enum.Parse(typeof(AutoOffsetReset), _configuration["ConfigProperties:Kafka:AutoOffsetReset"]);
                ConsumerConfig.EnableAutoCommit = true;
                _kafkaConsumer = new ConsumerBuilder<string, string>(ConsumerConfig).Build();
                _logger.LogInformation("Successfully constructed KafkaConsumer");
            }
           
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
                    EventWaitHandle lockObject = _synchronzationUtil.getLogObject(ConsumerRecord.Message.Key);
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
    }
}
