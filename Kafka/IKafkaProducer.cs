using com.bswift.model.events.employee;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.producer
{
    public interface IKafkaProducer
    {
        void ProduceRecord(string topicName, string key, EmployeeEvent value);
    }
}
