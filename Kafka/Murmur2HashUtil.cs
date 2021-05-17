using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.HashFunction.MurmurHash;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class Murmur2HashUtil : IMurmurHashUtil
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private readonly int NumPartitions;
		private readonly IMurmurHash2  _murmurHash2;

        public Murmur2HashUtil(ILogger<Murmur2HashUtil> logger, 
                        IConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            this.NumPartitions = _configuration.GetValue<int>("ConfigProperties:Kafka:ResponseTopicPartitions");
			_murmurHash2 = MurmurHash2Factory.Instance.Create();
        }
		/**
		 * Math.Abs throws overflow exception in some cases.
		 *  Making the 2's complement signed bit zero before
		 * determining the partition number
		 */
        public int MurmurHash(String key)
        {
			return (BitConverter.ToInt32(_murmurHash2.ComputeHash(Encoding.UTF8.GetBytes(key)).Hash, 0) & 0x7fffffff) % NumPartitions;
			//return (int) Hash(key) % NumPartitions;
        }

		public int MurmurHash(string key, int numPartitions)
		{
			return (BitConverter.ToInt32(_murmurHash2.ComputeHash(Encoding.UTF8.GetBytes(key)).Hash, 0) & 0x7fffffff) % numPartitions;
		}

		public int MurmurHash(byte[] keyData)
		{
			return (BitConverter.ToInt32(_murmurHash2.ComputeHash(keyData).Hash, 0) & 0x7fffffff) % NumPartitions;
		}

		public int MurmurHash(byte[] keyData, int numPartitions)
		{
			return (BitConverter.ToInt32(_murmurHash2.ComputeHash(keyData).Hash, 0) & 0x7fffffff) % numPartitions;
		}

    }
}
