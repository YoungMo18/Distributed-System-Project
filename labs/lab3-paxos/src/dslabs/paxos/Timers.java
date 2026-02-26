package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final int sequenceNum;
}

// Your code here...
@Data
final class HeartbeatTimer implements Timer {
  static final int HEARTBEAT_MILLIS = 25;
}

@Data
final class HeartbeatCheckTimer implements Timer {
  static final int HEARTBEAT_CHECK_MILLIS = 100;
}

@Data
final class ElectionRetryTimer implements Timer {
  static final int ELECTION_RETRY_MILLIS = 50;

  private final PaxosServer.Ballot ballot;
}

@Data
final class ProposalRetryTimer implements Timer {
  static final int PROPOSAL_RETRY_MILLIS = 50;

  private final PaxosServer.Ballot ballot;
  private final int slot;  // the slot for which the proposal is being retried
}
