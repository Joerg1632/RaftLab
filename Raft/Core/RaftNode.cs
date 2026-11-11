using Raft.Configuration;
using Raft.Dto;
using System.Diagnostics;

namespace Raft.Core;

public sealed class RaftNode
{
    private readonly Lock sync = new();
    private readonly string nodeId;
    private NodeRole role = NodeRole.Follower;
    private long currentTerm = 0;
    private string? votedFor;
    private string? leaderId;
    private readonly HashSet<string> memberIds;
    private readonly RaftOptions options;
    
    private long electionStartedAt;
    private int electionTimeoutMs;

    public RaftNode(RaftOptions options)
    {
        this.options = options;
        nodeId = options.NodeId;

        memberIds = options.Nodes.Keys.ToHashSet(
            StringComparer.Ordinal);

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
    
    private void MoveToHigherTerm(long term)
    {
        currentTerm = term;
        votedFor = null;
        role = NodeRole.Follower;
        leaderId = null;

        ResetElectionTimer();
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

            currentTerm = nextTerm;
            votedFor = nodeId;
            leaderId = null;
            role = NodeRole.Candidate;

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
            if (role != NodeRole.Candidate)
                return false;

            if (currentTerm != electionTerm)
                return false;

            var majority = memberIds.Count / 2 + 1;
            if (votes < majority)
                return false;

            role = NodeRole.Leader;
            leaderId = nodeId;

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

            if (votedFor is not null &&
                votedFor != request.CandidateId)
                return new VoteResponse(currentTerm, false);

            if (votedFor is null)
                votedFor = request.CandidateId;
            
            ResetElectionTimer();
            
            return new VoteResponse(currentTerm, true);
        }
    }
    
    public AppendEntriesResponse HandleAppendEntries(
        AppendEntriesRequest request)
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
            {
                return new AppendEntriesResponse(currentTerm, false);
            }

            if (request.Term > currentTerm)
            {
                MoveToHigherTerm(request.Term);
            }

            if (request.PrevLogIndex != 0 ||
                request.PrevLogTerm != 0 ||
                request.LeaderCommit != 0)
            {
                return new AppendEntriesResponse(currentTerm, false);
            }

            role = NodeRole.Follower;
            leaderId = request.LeaderId;

            ResetElectionTimer();

            return new AppendEntriesResponse(currentTerm, true);
        }
    }
}