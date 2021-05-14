using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.HashFunction.MurmurHash;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// there is some code duplication from MurmurHash2Util
// we can create a base class to avoid code duplication
namespace sampledotnetcoreapi.Kafka
{
    public class Murmur3HashUtil : IMurmurHashUtil
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private readonly IMurmurHash3 _murmurHash3;
        private readonly int NumPartitions;


        public Murmur3HashUtil(ILogger<Murmur3HashUtil> logger,
                            IConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            this.NumPartitions = _configuration.GetValue<int>("ConfigProperties:Kafka:ResponseTopicPartitions");
            _murmurHash3 = MurmurHash3Factory.Instance.Create();
        }
        public int MurmurHash(string key)
        {
            return Math.Abs(_murmurHash3.ComputeHash(Encoding.UTF8.GetBytes(key)).GetHashCode()) % NumPartitions;
        }

        public int MurmurHash(string key, int numPartitions)
        {
            return Math.Abs(_murmurHash3.ComputeHash(Encoding.UTF8.GetBytes(key)).GetHashCode()) % numPartitions;
        }

        public int MurmurHash(byte[] keyData)
        {
            return Math.Abs(_murmurHash3.ComputeHash(keyData).GetHashCode()) % NumPartitions;
        }

        public int MurmurHash(byte[] keyData, int numPartitions)
        {
            return Math.Abs(_murmurHash3.ComputeHash(keyData).GetHashCode()) % numPartitions;
        }
    }
}
