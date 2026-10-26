namespace Raft.Infrastructure;

public sealed record RaftPersistentState
(
    long CurrentTerm,
    string? VotedFor
);