using Raft.Configuration;
using Raft.Dto;
using Raft.Infrastructure;
using System.Diagnostics;

namespace Raft.Core;

public sealed class RaftNode
{
    private readonly Lock sync = new();
    private readonly List<LogEntry> log = new();
    private readonly string nodeId;
    private NodeRole role = NodeRole.Follower;
    private long currentTerm = 0;
    private string? votedFor;
    private string? leaderId;
    private readonly FileRaftStateStore store;
    private readonly HashSet<string> memberIds;
    private readonly RaftOptions options;
    private readonly Dictionary<string, long> nextIndex = new (StringComparer.Ordinal);
    private readonly Dictionary<string, long> matchIndex = new (StringComparer.Ordinal);
    
    private long electionStartedAt;
    private int electionTimeoutMs;

    public RaftNode(
        RaftOptions options,
        FileRaftStateStore store)
    {
        this.options = options;
        this.store = store;
        nodeId = options.NodeId;
        memberIds = options.Nodes.Keys.ToHashSet(
            StringComparer.Ordinal);

        var persistentState = store.Load();
        currentTerm = persistentState.CurrentTerm;
        votedFor = persistentState.VotedFor;
        log.AddRange(persistentState.Log ?? []);
        
        ResetElectionTimer();
    }
    
    private void ResetElectionTimer()
    {
        electionStartedAt = Stopwatch.GetTimestamp();

        electionTimeoutMs = Random.Shared.Next(
            options.ElectionTimeoutMinMs,
            options.ElectionTimeoutMaxMs
        );
    }
    
    private void PersistState(
        long term,
        string? vote,
        IReadOnlyList<LogEntry>? entries = null)
    {
        var state = new RaftPersistentState(
            term,
            vote,
            (entries ?? log).ToArray()
        );

        store.Save(state);
    }
    
    private void MoveToHigherTerm(long term)
    {
        PersistState(term, null);

        currentTerm = term;
        votedFor = null;
        role = NodeRole.Follower;
        leaderId = null;

        ClearReplicationState();
        ResetElectionTimer();
    }
    
    private void ClearReplicationState()
    {
        nextIndex.Clear();
        matchIndex.Clear();
    }
    
    public (long lastLogIndex, long lastLogTerm) GetLastLogInfo()
    {
        lock (sync)
        {
            if (log.Count == 0)
                return (0, 0);

            return (log.Count, log[^1].Term);
        }
    }
    
    public (long nextIndex, long matchIndex)? GetPeerReplicationState(string peerId)
    {
        lock (sync)
        {
            if (role != NodeRole.Leader)
                return null;

            if (!nextIndex.TryGetValue(peerId, out var next))
                return null;

            if (!matchIndex.TryGetValue(peerId, out var matched))
                return null;

            return (next, matched);
        }
    }

    public long? TryStartElection()
    {
        lock (sync)
        {
            if (role == NodeRole.Leader)
                return null;
            
            var elapsed = Stopwatch.GetElapsedTime(electionStartedAt);
            if (elapsed.TotalMilliseconds < electionTimeoutMs)
                return null;
            
            var nextTerm = checked(currentTerm + 1);
            PersistState(nextTerm, nodeId);

            currentTerm = nextTerm;
            votedFor = nodeId;
            leaderId = null;
            role = NodeRole.Candidate;
            
            ClearReplicationState();
            ResetElectionTimer();

            return currentTerm;
        }
    }
    
    public long? GetLeaderTerm()
    {
        lock (sync)
        {
            return role == NodeRole.Leader
                ? currentTerm
                : null;
        }
    }
    
    public void ObserveHigherTerm(long term)
    {
        lock (sync)
        {
            if (term > currentTerm)
                MoveToHigherTerm(term);
        }
    }
    
    public bool TryBecomeLeader(long electionTerm, int votes)
    {
        lock (sync)
        {
            if (role != NodeRole.Candidate || currentTerm != electionTerm)
                return false;

            var majority = memberIds.Count / 2 + 1;
            if (votes < majority)
                return false;

            role = NodeRole.Leader;
            leaderId = nodeId;

            var firstUnsentIndex = (long)log.Count + 1;
            foreach (var peerId in memberIds.Where(peerId => peerId != nodeId))
            {
                nextIndex[peerId] = firstUnsentIndex;
                matchIndex[peerId] = 0;
            }

            return true;
        }
    }

    public RaftStatus GetStatus()
    {
        lock (sync)
        {
            return new RaftStatus(
                nodeId,
                role.ToString(),
                currentTerm,
                votedFor,
                leaderId
            );
        }
    }

    public VoteResponse HandleRequestVote(VoteRequest request)
    {
        lock (sync)
        {
            if (string.IsNullOrWhiteSpace(request.CandidateId) ||
                !memberIds.Contains(request.CandidateId))
                return new VoteResponse(currentTerm, false);
            
            if (request.Term < currentTerm)
                return new VoteResponse(currentTerm, false);
            
            if (request.Term > currentTerm)
                MoveToHigherTerm(request.Term);

            if (request.LastLogIndex < 0 ||
                request.LastLogTerm < 0)
                return new VoteResponse(currentTerm, false);
            
            var localLastLogIndex = log.Count;
            var localLastLogTerm = log.Count == 0
                ? 0
                : log[^1].Term;

            var candidateUpToDate =
                request.LastLogTerm > localLastLogTerm ||
                (request.LastLogTerm == localLastLogTerm &&
                 request.LastLogIndex >= localLastLogIndex);

            if (!candidateUpToDate)
                return new VoteResponse(currentTerm, false);

            if (votedFor is null)
            {
                PersistState(currentTerm, request.CandidateId);
                votedFor = request.CandidateId;
            }
            
            ResetElectionTimer();
            
            return new VoteResponse(currentTerm, true);
        }
    }
    
    
    public AppendEntriesResponse HandleAppendEntries(AppendEntriesRequest request)
    {
        lock (sync)
        {
            if (string.IsNullOrWhiteSpace(request.LeaderId) ||
                !memberIds.Contains(request.LeaderId) ||
                request.LeaderId == nodeId)
            {
                return new AppendEntriesResponse(currentTerm, false);
            }

            if (request.Term < currentTerm)
                return new AppendEntriesResponse(currentTerm, false);

            if (request.Term > currentTerm)
                MoveToHigherTerm(request.Term);

            if (request.PrevLogIndex < 0 ||
                request.PrevLogTerm < 0 ||
                request.Entries is null ||
                request.LeaderCommit != 0 ||
                (request.PrevLogIndex == 0 &&
                 request.PrevLogTerm != 0))
            {
                return new AppendEntriesResponse(currentTerm, false);
            }

            foreach (var entry in request.Entries)
            {
                if (entry is null ||
                    entry.Term <= 0 ||
                    entry.Term > request.Term)
                {
                    return new AppendEntriesResponse(currentTerm, false);
                }
            }

            ClearReplicationState();
            role = NodeRole.Follower;
            leaderId = request.LeaderId;
            ResetElectionTimer();

            if (request.PrevLogIndex > log.Count)
                return new AppendEntriesResponse(currentTerm, false);

            if (request.PrevLogIndex > 0)
            {
                var prevPosition = checked((int)request.PrevLogIndex - 1);
                if (log[prevPosition].Term != request.PrevLogTerm)
                    return new AppendEntriesResponse(currentTerm, false);
            }

            var updatedLog = new List<LogEntry>(log);
            var logPosition = checked((int)request.PrevLogIndex);
            var incomingPosition = 0;
            
            while (incomingPosition < request.Entries.Count &&
                   logPosition < updatedLog.Count &&
                   updatedLog[logPosition].Term ==
                   request.Entries[incomingPosition].Term)
            {
                logPosition++;
                incomingPosition++;
            }
            
            if (incomingPosition < request.Entries.Count)
            {
                if (logPosition < updatedLog.Count)
                {
                    updatedLog.RemoveRange(
                        logPosition,
                        updatedLog.Count - logPosition
                    );
                }

                for (var i = incomingPosition;
                     i < request.Entries.Count;
                     i++)
                {
                    updatedLog.Add(request.Entries[i]);
                }

                PersistState(currentTerm, votedFor, updatedLog);
                log.Clear();
                log.AddRange(updatedLog);
            }

            return new AppendEntriesResponse(currentTerm, true);
        }
    }
}