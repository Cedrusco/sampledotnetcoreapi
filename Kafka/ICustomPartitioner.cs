using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface ICustomPartitioner
    {
        public Partition customPartitioner(string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull);


    }
}
