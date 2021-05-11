using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using sampledotnetcoreapi.Kafka;
using sampledotnetcoreapi.producer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Controllers
{
    [Route("api/employees")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly IKafkaProducer _producer;
        private readonly IKafkaConsumer _consumer;

        private readonly string TopicName;
        private Thread consumerThread;
        private readonly ISynchronzationUtil _synchronzationUtil;
        private readonly IMurmurHashUtil _murmurHashUtil;


        public KafkaProducerController(IConfiguration Configuration, 
                    ILogger<KafkaProducerController> Logger,
                    IKafkaProducer Producer,
                    ISynchronzationUtil SynchronzationUtil,
                    IKafkaConsumer Consumer,
                    IMurmurHashUtil MurmurHashUtil)
        {
            this._configuration = Configuration;
            this._logger = Logger;
            this._producer = Producer;
            this._synchronzationUtil = SynchronzationUtil;
            this._consumer = Consumer;
            this._murmurHashUtil = MurmurHashUtil;
            consumerThread = new Thread(_consumer.startConsumer);
            consumerThread.Start();
            TopicName = _configuration["ConfigProperties:Kafka:TopicName"];
        }

        [Route("")]
        [HttpPost]
        public async Task<IActionResult> post()
        {
            try
            {
                // This needs to be checked against the partition assignment of response topic consumer
                // in future enhancements
                string requestId = Guid.NewGuid().ToString();
                // This could affect response time 
                while (!_consumer.isAssignmentPartition(_murmurHashUtil.murmurHash(requestId))) {
                    requestId = Guid.NewGuid().ToString();
                }
                _logger.LogInformation("Computed request id using assigned partition from response topic {requestId}", requestId);
                var reader = new StreamReader(Request.Body);
                var value = await reader.ReadToEndAsync();
                _logger.LogInformation("Producing to {topicName}, key= {requestId}, value= {value}", TopicName, requestId, value);
                 _producer.ProduceRecord(TopicName, requestId, value);
                EventWaitHandle syncObject = new AutoResetEvent(false);

                _synchronzationUtil.addLockObject(requestId, syncObject);
                syncObject.WaitOne();
                return Ok(_consumer.getResponseById(requestId));
            }
            catch (Exception e)
            {
                _logger.LogError("Exception writing to kafka  message  '{message}'", e.Message);
                return this.StatusCode(StatusCodes.Status500InternalServerError, "Exception writing to kafka");
            }
        }
    }
}
