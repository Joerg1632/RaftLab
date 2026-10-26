using System.Text.Json;

namespace Raft.Infrastructure;

public sealed class FileRaftStateStore
{
    private readonly string path;

    public FileRaftStateStore(string path)
    {
        this.path = path;
    }

    public RaftPersistentState Load()
    {
        if (!File.Exists(path))
        {
            return new RaftPersistentState(0, null);
        }

        var json = File.ReadAllText(path);
        
        return JsonSerializer.Deserialize<RaftPersistentState>(json)
            ?? throw new InvalidDataException(
                "Invalid Raft persistent state");
    }

    public void Save(RaftPersistentState state)
    {
        Directory.CreateDirectory(
            Path.GetDirectoryName(path)!);
        
        var tempPath = path + ".tmp";
        using (var stream = new FileStream(
                   tempPath,
                   FileMode.Create,
                   FileAccess.Write,
                   FileShare.None))
        {
            JsonSerializer.Serialize(stream, state);
            stream.Flush(flushToDisk: true);
        }
        
        File.Move(tempPath, path, overwrite: true);
    }
}