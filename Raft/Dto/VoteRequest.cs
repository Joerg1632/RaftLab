namespace Raft.Dto;

public sealed record VoteRequest(
    long Term,
    string CandidateId,
    long LastLogIndex,
    long LastLogTerm
);