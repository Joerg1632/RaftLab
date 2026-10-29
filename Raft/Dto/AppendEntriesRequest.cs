using Raft.Core;

namespace Raft.Dto;

public sealed record AppendEntriesRequest(
    long Term,
    string LeaderId,
    long PrevLogIndex,
    long PrevLogTerm,
    IReadOnlyList<LogEntry> Entries,
    long LeaderCommit
);