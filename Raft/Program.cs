using Raft.Configuration;
using Raft.Core;
using Raft.Dto;
using Raft.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration
    .GetSection(RaftOptions.SectionName)
    .Get<RaftOptions>()
    ?? throw new InvalidOperationException("Raft configuration is missing");

if (string.IsNullOrWhiteSpace(options.NodeId) ||
    !options.Nodes.ContainsKey(options.NodeId))
{
    throw new InvalidOperationException(
        $"Unknown Raft node: {options.NodeId}");
}

if (options.ElectionTimeoutMinMs <= 0 ||
    options.ElectionTimeoutMaxMs <= options.ElectionTimeoutMinMs)
{
    throw new InvalidOperationException(
        "Invalid election timeout range.");
}

if (options.HeartbeatIntervalMs <= 0 ||
    options.HeartbeatIntervalMs >= options.ElectionTimeoutMinMs)
{
    throw new InvalidOperationException(
        "Invalid heartbeat interval.");
}

builder.WebHost.UseUrls(options.Nodes[options.NodeId]);
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<RaftNode>();
builder.Services.AddHostedService<RaftWorker>();

var statePath = Path.Combine(
    builder.Environment.ContentRootPath,
    "Data",
    options.NodeId,
    "state.json"
);

builder.Services.AddSingleton(
    new FileRaftStateStore(statePath));

builder.Services.AddHttpClient("raft", client =>
{
    client.Timeout = TimeSpan.FromMilliseconds(700);
});

builder.Services.AddHostedService<RaftHeartbeatWorker>();

var app = builder.Build();

app.MapGet("/status", (RaftNode node) =>
{
    return Results.Ok(node.GetStatus());
});

app.MapPost("/raft/request-vote",
    (VoteRequest request, RaftNode node) =>
    {
        var response = node.HandleRequestVote(request);

        return Results.Ok(response);
    });

app.MapPost("/raft/append-entries",
    (AppendEntriesRequest request, RaftNode node) =>
    {
        var response = node.HandleAppendEntries(request);

        return Results.Ok(response);
    });

app.MapGet("/debug/node", (RaftOptions raft) =>
{
    return Results.Ok(new
    {
        raft.NodeId,
        Address = raft.Nodes[raft.NodeId],

        Peers = raft.Nodes
            .Where(node => node.Key != raft.NodeId)
            .ToDictionary(node => node.Key, node => node.Value)
    });
});
    
app.Run();    