﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.producer
{
    public interface IKafkaProducer
    {
        void ProduceRecord(string TopicName, string key, string value);
    }
}
