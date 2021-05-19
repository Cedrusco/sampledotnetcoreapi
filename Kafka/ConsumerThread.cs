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
        private  IKafkaConsumer _kafkaConsumer;

        public ConsumerThread(ILogger<ConsumerThread> logger, IKafkaConsumer kafkaConsumer)
        {
            this._logger = logger;
            this._kafkaConsumer = kafkaConsumer;
        }
        public void StartConsumerThread()
        {
            if (_consumerThread != null)
            {
                _consumerThread = new Thread(_kafkaConsumer.StartConsumer);
                _consumerThread.Start();
            }
        }
    }
}
