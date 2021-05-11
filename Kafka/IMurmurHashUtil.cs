using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface IMurmurHashUtil
    {
        public int murmurHash(String key);
    }
}
