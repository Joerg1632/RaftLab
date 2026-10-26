namespace Raft.Dto;

public sealed record AppendEntriesResponse(
    long Term,
    bool Success
);