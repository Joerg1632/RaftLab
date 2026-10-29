namespace Raft.Core;

public record LogEntry(
    long Term,
    string Command
);