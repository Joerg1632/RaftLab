namespace Raft.Dto;

public sealed record AppendEntriesRequest(
    long Term,
    string LeaderId,
    long PrevLogIndex,
    long PrevLogTerm,
    long LeaderCommit
);