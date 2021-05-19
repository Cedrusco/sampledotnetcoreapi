using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class ConfigUtil : IConfigUtil
    {
        private readonly ILogger _logger;

        public ConfigUtil(ILogger<ConfigUtil> logger)
        {
            this._logger = logger;
            _logger.LogInformation("Constructor called");
        }
        public async Task<ClientConfig> LoadConfig(string fileName, string caLocation)
        {
                try
                {
                    var kafkaConfigPropertiesMap = (await File.ReadAllLinesAsync(fileName))
                                    .Where(line => !line.StartsWith("#"))
                                    .ToDictionary(line => line.Substring(0, line.IndexOf('=')),
                                                    line => line.Substring(line.IndexOf('=') + 1));

                    ClientConfig kafkaConfig = new ClientConfig(kafkaConfigPropertiesMap);

                    if (caLocation != null)
                    {
                        kafkaConfig.SslCaLocation = caLocation;
                    }

                    return kafkaConfig;
                }
                catch (Exception e)
                {
                    _logger.LogError("Error reading the kafka configuration filename= '{FileName}', '{Message}'", fileName, e.Message);
                    // need to refactor later
                    return null;
                }

            }
    }
}
