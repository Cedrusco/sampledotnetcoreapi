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

        private readonly string topicName;
        private Thread consumerThread;
        private readonly ISynchronzationUtil _synchronzationUtil;
        private readonly IMurmurHashUtil _murmur2HashUtil;


        public KafkaProducerController(IConfiguration configuration, 
                    ILogger<KafkaProducerController> logger,
                    IKafkaProducer producer,
                    ISynchronzationUtil synchronzationUtil,
                    IKafkaConsumer consumer,
                    IMurmurHashUtil murmur2HashUtil)
        {
            this._configuration = configuration;
            this._logger = logger;
            this._producer = producer;
            this._synchronzationUtil = synchronzationUtil;
            this._consumer = consumer;
            this._murmur2HashUtil = murmur2HashUtil;
            consumerThread = new Thread(_consumer.StartConsumer);
            consumerThread.Start();
            topicName = _configuration["ConfigProperties:Kafka:TopicName"];
        }

        [Route("")]
        [HttpPost]
        public async Task<IActionResult> post()
        {
            try
            {
                // This needs to be checked against the partition assignment of response topic consumer
                // in future enhancements
                // This could affect response time 
                var computePartition = _configuration.GetValue<bool>("ConfigProperties:Kafka:ComputePartition");
                string requestId = _consumer.GenerateRequestId(computePartition);
                _logger.LogInformation("Computed request id using assigned partition from response topic {requestId}", requestId);
                var reader = new StreamReader(Request.Body);
                var value = await reader.ReadToEndAsync();
                _logger.LogInformation("Producing to {topicName}, key= {requestId}, value= {value}", topicName, requestId, value);
                 _producer.ProduceRecord(topicName, requestId, value);
                EventWaitHandle syncObject = new AutoResetEvent(false);
                _synchronzationUtil.AddLockObject(requestId, syncObject);
                syncObject.WaitOne();
                return Ok(_consumer.GetResponseById(requestId));
            }
            catch (Exception e)
            {
                _logger.LogError("Exception writing to kafka  message  {message}, stack trace {stack}", e.Message, e.StackTrace);
                return this.StatusCode(StatusCodes.Status500InternalServerError, "Exception writing to kafka");
            }
        }
    }
}
