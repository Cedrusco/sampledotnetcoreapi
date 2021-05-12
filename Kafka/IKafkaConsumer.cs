using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface IKafkaConsumer
    {
        String GetResponseById(String requestId);
        void StartConsumer();

        bool IsAssignmentPartition(int partitionId);
    }
}
