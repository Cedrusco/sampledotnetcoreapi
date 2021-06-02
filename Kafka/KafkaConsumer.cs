using com.bswift.model.events.employee;
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
        private IConsumer<string, EmployeeEvent> _kafkaConsumer;
        private readonly IConfigUtil _configUtil;
        private readonly ISynchronzationUtil _synchronzationUtil;
        private readonly IMurmurHashUtil _murmurHashUtil;

        private readonly string _topicName;
        private ConcurrentDictionary<string, string> _responseMap = new ConcurrentDictionary<string, string>();
        private Thread consumerThread;

        public KafkaConsumer(ILogger<KafkaConsumer> logger,
                                IConfiguration configuration,
                                   IConfigUtil configUtil,
                                   ISynchronzationUtil synchronzationUtil,
                                   IMurmurHashUtil murmurHashUtil)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._configUtil = configUtil;
            this._synchronzationUtil = synchronzationUtil;
            this._murmurHashUtil = murmurHashUtil;
            _topicName = _configuration["ConfigProperties:Kafka:ResponseTopicName"];
            EnsureConsumer();
            _logger.LogInformation("Constructor called");
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
            /** for test
            if (partitionId == 0)
            {
                return true;
            }
            **/
            foreach( var Assignment in _kafkaConsumer.Assignment)
            {
                if (Assignment.Partition.Equals(partitionId) && 
                        Assignment.Topic.Equals(_topicName))
                {
                    _logger.LogInformation("matching partition id : {partitionId} ", partitionId);
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
                    try
                    {
                        var ConsumerRecord = _kafkaConsumer.Consume(cts.Token);
                        _logger.LogInformation("Consumed the record key = {Key}, value = {Value}",
                            ConsumerRecord.Message.Key, ConsumerRecord.Message.Value);
                        // Get the lock object for request
                        if (ConsumerRecord.Message.Value.status == StatusType.COMPLETED)
                        {
                            EventWaitHandle lockObject = _synchronzationUtil.GetLockObject(ConsumerRecord.Message.Key);
                            if (lockObject != null)
                            {
                                if (!_responseMap.TryAdd(ConsumerRecord.Message.Key, ConsumerRecord.Message.Value.status.ToString()))
                                {
                                    _logger.LogWarning("Error adding response to response map for request {Key}", ConsumerRecord.Message.Key);
                                }
                                lockObject.Set(); // Signal the main controller thread so that it can get the response
                            }
                        }
                    }
                    catch (ConsumeException consumeException)
                    {
                        string key = BitConverter.ToString(consumeException.ConsumerRecord.Message.Key);
                        _responseMap.TryAdd(key, StatusType.FAILED.ToString());
                        _synchronzationUtil.GetLockObject(key).Set();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                //commit offset manually
                // try
                // {
                //     _kafkaConsumer.Commit();
                // }
                // catch (KafkaException e)
                // {
                //     _logger.LogWarning("Exceptiopn while committing the offsets on ctrl-c {message}", e.Message);
                //  }
            }
            finally
            {
                //this commits the offsets
                _kafkaConsumer.Close();
            }
        }

        private  void EnsureConsumer()
        {
            if (_kafkaConsumer == null)
            {
                var config =  _configUtil.LoadConfig();
                var consumerConfig = new ConsumerConfig(config);
                // INSTANCEID can be omitted depending upon how we choose requestId
                consumerConfig.GroupId = _configuration["ConfigProperties:Kafka:ConsumerGroupId"];
                var computePartition = _configuration.GetValue<bool>("ConfigProperties:Kafka:ComputePartition");
                if (computePartition)
                {
                    consumerConfig.GroupId += Environment.GetEnvironmentVariable("INSTANCE_ID");
                }                
                consumerConfig.AutoOffsetReset = (AutoOffsetReset)Enum.Parse(typeof(AutoOffsetReset), _configuration["ConfigProperties:Kafka:AutoOffsetReset"]);
                consumerConfig.EnableAutoCommit = true;
                _kafkaConsumer = new ConsumerBuilder<string, EmployeeEvent>(consumerConfig)
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(SchemaRegistryUtil.GetDeserializer())
                    .Build();
                (consumerThread = new Thread(StartConsumer)).Start();
                _logger.LogInformation("Successfully constructed KafkaConsumer and started consumer thread");
            }
        }

        public string GenerateRequestId(bool computePartition)
        {
            string requestId = Guid.NewGuid().ToString();
            if (computePartition)
            {
                while (!IsAssignmentPartition(_murmurHashUtil.MurmurHash(requestId)))
                {
                    requestId = Guid.NewGuid().ToString();
                }
            }

            return requestId;
        }
    }
}
