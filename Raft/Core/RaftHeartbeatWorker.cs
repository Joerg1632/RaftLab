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

                await SendHeartbeatsAsync(
                    term.Value,
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Приложение завершает работу.
        }
    }
    
    private async Task SendHeartbeatsAsync(
        long term,
        CancellationToken cancellationToken)
    {
        var request = new AppendEntriesRequest(
            term,
            options.NodeId,
            0,
            0,
            Array.Empty<LogEntry>(),
            0
        );

        var tasks = options.Nodes
            .Where(peer => peer.Key != options.NodeId)
            .Select(peer => SendHeartbeatAsync(
                peer.Key,
                peer.Value,
                request,
                cancellationToken
            ))
            .ToArray();

        await Task.WhenAll(tasks);
    }
    
    private async Task SendHeartbeatAsync(
        string peerId,
        string address,
        AppendEntriesRequest request,
        CancellationToken cancellationToken)
    {
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
                    cancellationToken: cancellationToken);

            if (result is null)
            {
                logger.LogWarning(
                    "Empty heartbeat response from {PeerId}",
                    peerId);

                return;
            }

            if (result.Term > request.Term)
            {
                node.ObserveHigherTerm(result.Term);
            }

            if (!result.Success)
            {
                logger.LogDebug(
                    "Heartbeat rejected by {PeerId} in term {Term}",
                    peerId,
                    result.Term);
            }
        }
        catch (HttpRequestException ex)
        {
            logger.LogWarning(
                "Heartbeat to {PeerId} failed: {Message}",
                peerId,
                ex.Message);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                "Invalid heartbeat response from {PeerId}: {Message}",
                peerId,
                ex.Message);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "Heartbeat to {PeerId} timed out",
                peerId);
        }
    }
}