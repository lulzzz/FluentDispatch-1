﻿using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using MagicOnion;
using Microsoft.Extensions.Logging;
using GrandCentralDispatch.Models;
using GrandCentralDispatch.Resolvers;

namespace GrandCentralDispatch.Sample.Remote.Contract.Resolvers
{
    public sealed class UriResolver : Item2RemotePartialResolver<Uri, string>
    {
        private readonly ILogger _logger;
        private readonly HttpClient _httpClient;

        public UriResolver(ILoggerFactory loggerFactory, IHttpClientFactory httpClientFactory)
        {
            _httpClient = httpClientFactory.CreateClient();
            _logger = loggerFactory.CreateLogger<UriResolver>();
        }

        /// <summary>
        /// Process each new URI and download its content
        /// </summary>
        /// <param name="item"><see cref="KeyValuePair{TKey,TValue}"/></param>
        /// <param name="nodeMetrics"><see cref="NodeMetrics"/></param>
        /// <returns><see cref="Task"/></returns>
        public override async UnaryResult<string> ProcessItem2Remotely(Uri item,
            NodeMetrics nodeMetrics)
        {
            _logger.LogInformation(
                $"New URI {item.AbsoluteUri} received, trying to download content from node {nodeMetrics.Id}...");
            var response =
                await _httpClient.GetAsync(item, CancellationToken.None);
            return await response.Content.ReadAsStringAsync();
        }
    }
}