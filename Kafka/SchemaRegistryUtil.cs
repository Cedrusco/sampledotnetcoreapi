using com.bswift.model.events.employee;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace sampledotnetcoreapi.Kafka
{
    public class SchemaRegistryUtil
    {
        private static readonly string schemaRegistryURL = "https://psrc-4xgzx.us-east-2.aws.confluent.cloud";
        public static IDeserializer<EmployeeEvent> GetDeserializer()
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryURL,
                BasicAuthUserInfo = "CUXIJ3GC5X4TPREW:aX7pqhnvDl8heSJRD1tDXMJAxVjTzdoGoXYe8a4tBCW6hqbwLDCxL54xCpNGfJEU"
            };
            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            return new AvroDeserializer<EmployeeEvent>(schemaRegistry).AsSyncOverAsync();
        }

        public static ISerializer<EmployeeEvent> GetSerializer()
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryURL,
                BasicAuthUserInfo = "CUXIJ3GC5X4TPREW:aX7pqhnvDl8heSJRD1tDXMJAxVjTzdoGoXYe8a4tBCW6hqbwLDCxL54xCpNGfJEU"
            };
            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            return new AvroSerializer<EmployeeEvent>(schemaRegistry).AsSyncOverAsync();
        }

        public static IDeserializer<AuditEvent> GetAuditDeserializer()
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryURL,
                BasicAuthUserInfo = "CUXIJ3GC5X4TPREW:aX7pqhnvDl8heSJRD1tDXMJAxVjTzdoGoXYe8a4tBCW6hqbwLDCxL54xCpNGfJEU"
            };
            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            return new AvroDeserializer<AuditEvent>(schemaRegistry).AsSyncOverAsync();
        }
         
        public static ISerializer<AuditEvent> GetAuditSerializer()
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryURL,
                BasicAuthUserInfo = "CUXIJ3GC5X4TPREW:aX7pqhnvDl8heSJRD1tDXMJAxVjTzdoGoXYe8a4tBCW6hqbwLDCxL54xCpNGfJEU"
            };
            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            return new AvroSerializer<AuditEvent>(schemaRegistry).AsSyncOverAsync();
        }
    }

}
