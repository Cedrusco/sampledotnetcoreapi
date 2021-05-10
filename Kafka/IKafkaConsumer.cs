using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface IKafkaConsumer
    {
        String getResponseById(String requestId);
        void startConsumer();

        bool isAssignmentPartition(int partitionId);
    }
}
