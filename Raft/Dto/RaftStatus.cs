namespace Raft.Dto;

public sealed record RaftStatus(
    string NodeId,
    string Role,
    long CurrentTerm,
    string? VotedFor,
    string? LeaderId
);