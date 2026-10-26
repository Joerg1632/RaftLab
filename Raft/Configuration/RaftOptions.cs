namespace Raft.Configuration;

public sealed class RaftOptions
{
    public const string SectionName = "Raft";
    public string NodeId { get; set; } = string.Empty;
    public Dictionary<string, string> Nodes { get; set; } = new();
    public int ElectionTimeoutMinMs { get; set; } = 1500;
    public int ElectionTimeoutMaxMs { get; set; } = 3000;
    public int HeartbeatIntervalMs { get; set; } = 300;
}