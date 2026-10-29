using System.Text.Json;
using Raft.Configuration;
using Raft.Dto;

namespace Raft.Core;

public sealed class RaftWorker : BackgroundService
{
    private readonly RaftNode node;
    private readonly RaftOptions options;
    private readonly ILogger<RaftWorker> logger;
    private readonly HttpClient httpClient;

    public RaftWorker(
        RaftNode node,
        RaftOptions options,
        ILogger<RaftWorker> logger,
        IHttpClientFactory httpClientFactory)
    {
        this.node = node;
        this.options = options;
        this.logger = logger;
        httpClient = httpClientFactory.CreateClient("raft");
    }
    
    private async Task<VoteResponse?> RequestVoteAsync(
        string peerId,
        string address,
        VoteRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var url = $"{address.TrimEnd('/')}/raft/request-vote";

            using var response = await httpClient.PostAsJsonAsync(
                url,
                request,
                cancellationToken
            );

            response.EnsureSuccessStatusCode();

            return await response.Content
                .ReadFromJsonAsync<VoteResponse>(
                    cancellationToken: cancellationToken
                );
        }
        catch (HttpRequestException ex)
        {
            logger.LogWarning(
                "RequestVote to {PeerId} failed: {Message}",
                peerId,
                ex.Message
            );

            return null;
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                "Invalid RequestVote response from {PeerId}: {Message}",
                peerId,
                ex.Message
            );

            return null;
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "RequestVote to {PeerId} timed out",
                peerId
            );

            return null;
        }
    }
    
    private async Task RunElectionAsync(
        long electionTerm,
        CancellationToken cancellationToken)
    {
        var (lastLogIndex, lastLogTerm) = node.GetLastLogInfo();
        var tasks = options.Nodes
            .Where(peer => peer.Key != options.NodeId)
            .Select(peer => RequestVoteAsync(
                peer.Key,
                peer.Value,
                new VoteRequest(electionTerm, options.NodeId, lastLogIndex, lastLogTerm),
                cancellationToken
            ))
            .ToArray();

        var responses = await Task.WhenAll(tasks);
        var votes = 1;
        foreach (var response in responses)
        {
            if (response is null)
                continue;

            if (response.Term > electionTerm)
            {
                node.ObserveHigherTerm(response.Term);
                continue;
            }

            if (response.Term == electionTerm &&
                response.VoteGranted)
            {
                votes++;
            }
        }

        if (node.TryBecomeLeader(electionTerm, votes))
        {
            logger.LogInformation(
                "Node {NodeId} became Leader in term {Term} with {Votes} votes",
                options.NodeId,
                electionTerm,
                votes
            );
        }
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(50));
        
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                var term = node.TryStartElection();

                if (term.HasValue)
                {
                    logger.LogInformation(
                        "Node {NodeId} started election in term {Term}",
                        options.NodeId,
                        term.Value);
                    
                    await RunElectionAsync(term.Value, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Штатная остановка приложения.
        }
    }
}