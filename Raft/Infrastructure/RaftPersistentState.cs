using Microsoft.Extensions.Logging.Abstractions;
using Raft.Core;

namespace Raft.Infrastructure;

public sealed record RaftPersistentState (
    long CurrentTerm,
    string? VotedFor,
    LogEntry[]? Log = null
);