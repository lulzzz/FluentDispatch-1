using System;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using FluentDispatch.Contract.Helpers;
using FluentDispatch.Contract.Models.ElasticSearch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Nest;

namespace FluentDispatch.Contract.Services.ElasticSearch
{
    public class ElasticSearchService : IElasticSearchService
    {
        public Lazy<Task<IElasticClient>> Client { get; }
        private readonly ILogger _logger;

        public ElasticSearchService(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<ElasticSearchService>();
            Client = new Lazy<Task<IElasticClient>>(async () =>
            {
                var pool = new SingleNodeConnectionPool(
                    new Uri(configuration.GetValue<string>("ELASTICSEARCH_ENDPOINT")));
                var connSettings = new ConnectionSettings(pool)
                    .IncludeServerStackTraceOnError()
                    .EnableHttpPipelining()
                    .EnableHttpCompression();
                var client = new ElasticClient(connSettings);
                try
                {
                    var mapping =
                        new CreateIndexDescriptor(Constants.ReviewIndexName)
                            .Map<Review>(m => m
                                .AutoMap()
                            );

                    var existResponse = await client.Indices.ExistsAsync(Constants.ReviewIndexName);
                    if (!existResponse.Exists)
                    {
                        await client.Indices.CreateAsync(mapping);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }

                return client;
            }, LazyThreadSafetyMode.ExecutionAndPublication);
        }
    }
}