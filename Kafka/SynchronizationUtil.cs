﻿using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class SynchronizationUtil : ISynchronzationUtil
    {

        private ILogger _logger;
        private ConcurrentDictionary<string, EventWaitHandle> lockObjectMap = new ConcurrentDictionary<string, EventWaitHandle>();

        public SynchronizationUtil(ILogger<SynchronizationUtil> Logger)
        {
            this._logger = Logger;
        }
        public void addLockObject(string requestId, EventWaitHandle lockObject)
        {

            if (!lockObjectMap.TryAdd(requestId, lockObject))
            {
                _logger.LogError("Error adding the lock object for requestId {requestId}");
            }
        }

        public EventWaitHandle getLogObject(string requestId)
        {
            EventWaitHandle lockObject = null;
            if (!lockObjectMap.TryRemove(requestId, out lockObject))
            {
                _logger.LogError("Error getting the lock object for requestId {requestId}");
            }
            return lockObject;
        }

        public void removeLockObject(string requestId)
        {
            throw new NotImplementedException();
        }
    }
}
