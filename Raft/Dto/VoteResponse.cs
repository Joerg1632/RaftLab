namespace Raft.Dto;

public sealed record VoteResponse(
    long Term,
    bool VoteGranted
);