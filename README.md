# Raft

- 发起选举
  - follower一段时间内没有收到心跳（sleep随机时间timeout，sleep结束后检查是否time.Now().Sub(lastElectionTime) > timeout，会转化状态为Candidate并自增term，投票给自己，并发起选举，并重置lastElectionTime
  - RequestVote字段包含自身term，id，lastLogIndex， lastLogTerm
  - 其他节点收到后会先比对term，自身term大于其term，直接返回，小于则会将状态转化为follower（会重置votedFor），然后再进行log的比对
  - 当（votedFor == -1 即没有投过票 || votedFor == CandidateId）&& Candidate的log更新,则votedFor == CandidateId，并重置lastElectionTime
  - Candidate收到reply后，首先检查reply.Term == currentTerm,只有相等,在能进行后续操作
  - 当收到大多数节点的投票(别忘了包含自己的投票),则成为leader,并设置matchIndex 和 nextIndex,初始nextIndex为len(log)+1(日志index从1开始)
  - 成为leader后立即发送心跳

- 发送心跳/日志
  - 每隔随机时间,便向其他节点发送心跳,注意在每个心跳发送前需要确认当前状态依然是leader
  - 发送心跳时需判断term是否改变,状态是否改变.
  - AppendEntries字段包含term,id,LeaderCommitIndex,PrevLogIndex,PrevLogTerm,Entries
  - 当len(log) >= nextIndex时,添加entries
  - 当follower或Candidate收到心跳后,首先检查term,(Candidate收到同term发来的心跳时,需要变为follower),若匹配,再进行后续操作
  - 当PrevLogIndex != 0 且 当前节点log,PrevLogIndex处的term != PrevLogTerm (包含PreLogIndex处无日志) 直接返回false(可返回该term下第一个日志的index)此时也需要重置该节点的lastElectionTime
  - 当leaderCommitIndex大于commitIndex时,commitIndex = min(leaderCommitIndex, len(log)),并apply相应log,直到lastApplied == commitIndex
  - follower上述操作处理完后,返回true并更新lastElectionTime
  - leader收到reply后检查term是否匹配
  - 检查当前状态是否为leader
  - 若成功，则更新对应节点的matchIndex，nextIndex。如果AppendEntries中Entries为nil，即没有发送日志，直接返回。
  - 检查matchIndex的值是否在大多数节点上，且leader当前term == match日志的term，则更新commitIndex = max（comitIndex，match）（leader不能commit自身term以外的日志）
  - 如果commitIndex小于lastApplied，并apply相应log,直到lastApplied == commitIndex


# TODO
- 异步apply
- match计算




