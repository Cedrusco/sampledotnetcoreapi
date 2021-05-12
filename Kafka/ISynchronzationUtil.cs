using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface ISynchronzationUtil
    {
        public void AddLockObject(string requestId, EventWaitHandle lockObject);
        public void RemoveLockObject(string requestId);

        public EventWaitHandle GetLockObject(string requestId);
    }
}
