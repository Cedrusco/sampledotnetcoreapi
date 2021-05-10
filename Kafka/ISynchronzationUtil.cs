using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public interface ISynchronzationUtil
    {
        public void addLockObject(string requestId, EventWaitHandle lockObject);
        public void removeLockObject(string requestId);

        public EventWaitHandle getLogObject(string requestId);
    }
}
