using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class CustomPartitioner : ICustomPartitioner
    {

        private readonly IMurmurHashUtil _murmurHashUtil;
        private readonly ILogger _logger;

        public CustomPartitioner(ILogger<CustomPartitioner> logger, IMurmurHashUtil murmurHashUtil)
        {
            this._murmurHashUtil = murmurHashUtil;
            this._logger = logger;
            _logger.LogInformation("Constructor called");
        }
        public Partition customPartitioner(string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull)
        {
            var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
            _logger.LogInformation("Key string in partitioner : {key}", keyString);
            return new Partition(_murmurHashUtil.MurmurHash(keyData.ToArray(), partitionCount));
        }
    }
}
