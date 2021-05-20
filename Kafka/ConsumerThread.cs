using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class ConsumerThread : IConsumerThread
    {
        private Thread _consumerThread;
        private readonly ILogger _logger;
        private IKafkaConsumer _consumer;

        public ConsumerThread(ILogger<ConsumerThread> logger, IKafkaConsumer consumer)
        {
            this._logger = logger;
            this._consumer = consumer;
            _logger.LogInformation("Constructor called");
        }
        public void StartConsumerThread()
        {
            if (_consumerThread != null)
            {
                _consumerThread = new Thread(_consumer.StartConsumer);
                _consumerThread.Start();
            }
        }
    }
}
