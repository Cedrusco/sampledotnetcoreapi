using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface IMurmurHashUtil
    {
        public int MurmurHash(string key);
        public int MurmurHash(string key, int numPartitions);
        public int MurmurHash(byte[] keyData);
        public int MurmurHash(byte[] keyData, int numPartitions);
    }
}
