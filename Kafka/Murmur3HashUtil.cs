using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class Murmur3HashUtil : IMurmurHashUtil
    {
        public int MurmurHash(string key)
        {
            throw new NotImplementedException();
        }

        public int MurmurHash(string key, int numPartitions)
        {
            throw new NotImplementedException();
        }
    }
}
