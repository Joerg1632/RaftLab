using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using Raft.Configuration;
using Raft.Dto;

namespace Raft.Core;

public sealed class RaftHeartbeatWorker : BackgroundService
{
    private readonly RaftNode node;
    private readonly RaftOptions options;
    private readonly HttpClient httpClient;
    private readonly ILogger<RaftHeartbeatWorker> logger;

    public RaftHeartbeatWorker(
        RaftNode node,
        RaftOptions options,
        IHttpClientFactory httpClientFactory,
        ILogger<RaftHeartbeatWorker> logger)
    {
        this.node = node;
        this.options = options;
        this.logger = logger;
        httpClient = httpClientFactory.CreateClient("raft");
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(50));

        long? lastSentTerm = null;
        long lastSentAt = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                var term = node.GetLeaderTerm();
                if (term is null)
                {
                    lastSentTerm = null;
                    continue;
                }

                if (lastSentTerm == term &&
                    Stopwatch.GetElapsedTime(lastSentAt) <
                    TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs))
                {
                    continue;
                }

                lastSentTerm = term;
                lastSentAt = Stopwatch.GetTimestamp();

                await ReplicateToFollowersAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            
        }
    }
    
    private async Task ReplicateToFollowersAsync(
        CancellationToken cancellationToken)
    {
        var tasks = options.Nodes
            .Where(peer => peer.Key != options.NodeId)
            .Select(peer => ReplicateToPeerAsync(
                peer.Key,
                peer.Value,
                cancellationToken
            ))
            .ToArray();

        await Task.WhenAll(tasks);
    }
    
    private async Task ReplicateToPeerAsync(
        string peerId,
        string address,
        CancellationToken cancellationToken)
    {
        var request = node.PrepareAppendEntries(peerId);
        if (request is null)
            return;

        try
        {
            var url = $"{address.TrimEnd('/')}/raft/append-entries";
            using var response = await httpClient.PostAsJsonAsync(
                url,
                request,
                cancellationToken
            );

            response.EnsureSuccessStatusCode();
            
            var result = await response.Content
                .ReadFromJsonAsync<AppendEntriesResponse>(
                    cancellationToken: cancellationToken
                );

            if (result is null)
            {
                logger.LogWarning(
                    "Empty AppendEntries response from {PeerId}",
                    peerId
                );
                return;
            }

            node.HandleAppendEntriesResult(
                peerId,
                request,
                result
            );

            if (!result.Success)
            {
                logger.LogDebug(
                    "AppendEntries rejected by {PeerId}, term {Term}",
                    peerId,
                    result.Term
                );
            }
        }
        catch (HttpRequestException ex)
        {
            logger.LogWarning(
                "Replication to {PeerId} failed: {Message}",
                peerId,
                ex.Message
            );
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                "Invalid response from {PeerId}: {Message}",
                peerId,
                ex.Message
            );
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "Replication to {PeerId} timed out",
                peerId
            );
        }
    }
}