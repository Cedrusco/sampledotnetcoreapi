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

        public ConfigUtil(ILogger<ConfigUtil> Logger)
        {
            this._logger = Logger;
        }
        public async Task<ClientConfig> LoadConfig(string FileName, string CaLocation)
        {
                try
                {
                    var kafkaConfigPropertiesMap = (await File.ReadAllLinesAsync(FileName))
                                    .Where(line => !line.StartsWith("#"))
                                    .ToDictionary(line => line.Substring(0, line.IndexOf('=')),
                                                    line => line.Substring(line.IndexOf('=') + 1));

                    ClientConfig kafkaConfig = new ClientConfig(kafkaConfigPropertiesMap);

                    if (CaLocation != null)
                    {
                        kafkaConfig.SslCaLocation = CaLocation;
                    }

                    return kafkaConfig;
                }
                catch (Exception e)
                {
                    _logger.LogError("Error reading the kafka configuration filename= '{FileName}', '{Message}'", FileName, e.Message);
                    // need to refactor later
                    return null;
                }

            }
    }
}
