using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Protocols;
using StackExchange.Redis;
using System;

namespace RestClientUP
{
    public class CASRedisConnectionFactory
    {
        private readonly Lazy<ConnectionMultiplexer> LazyConnection;

        public  CASRedisConnectionFactory(IConfiguration Configuration)
        {
            ConfigurationOptions _configurationOptions = new ConfigurationOptions()
            {
                EndPoints = { Configuration["RedisConnectionStrings:Primary"],Configuration["RedisConnectionStrings:Read"] },
                AbortOnConnectFail = false,
                ConnectTimeout = 30000,
                AllowAdmin = true,
                KeepAlive = 60,
                ConnectRetry = 3,
                ClientName = "UPProcessor",
                AsyncTimeout = 80000,
                SyncTimeout = 80000,
            };
            LazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_configurationOptions));
        }
        public ConnectionMultiplexer Connection => LazyConnection.Value;
    }
}
