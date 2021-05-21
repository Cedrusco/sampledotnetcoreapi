using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
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
        private readonly IConfiguration _configuration;

        public ConfigUtil(ILogger<ConfigUtil> logger, IConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Constructor called");
        }
        public  ClientConfig LoadConfig()
        {
                try
                {

                    var kafkaConfigPropertiesMap = new Dictionary<String, String>();
                    kafkaConfigPropertiesMap.Add("bootstrap.servers", _configuration["ConfigProperties:Kafka:bootstrapServers"]);
                    kafkaConfigPropertiesMap.Add("security.protocol", _configuration["ConfigProperties:Kafka:securityProtocol"]);
                    kafkaConfigPropertiesMap.Add("sasl.mechanisms", _configuration["ConfigProperties:Kafka:saslMechanisms"]);
                    kafkaConfigPropertiesMap.Add("sasl.username", _configuration["ConfigProperties:Kafka:saslUsername"]);
                    kafkaConfigPropertiesMap.Add("sasl.password", _configuration["ConfigProperties:Kafka:saslPassword"]);

                    ClientConfig kafkaConfig = new ClientConfig(kafkaConfigPropertiesMap);

                    return kafkaConfig;
                }
                catch (Exception e)
                {
                    _logger.LogError("Error reading the kafka configuration '{Message}'", e.Message);
                    // need to refactor later
                    return null;
                }

            }
    }
}
