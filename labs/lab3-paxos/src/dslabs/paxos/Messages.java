package dslabs.paxos;

// Your code here...

import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.paxos.PaxosServer.LogEntry;
import java.util.Map;
import lombok.Data;

@Data
class P1A implements Message {
  private final PaxosServer.Ballot ballot;
}

@Data
class P1B implements Message {
  private final PaxosServer.Ballot ballot;
  private final Map<Integer, PaxosServer.LogEntry> logSnapshot;
}

@Data
class P2A implements Message {
  private final PaxosServer.Ballot ballot;
  private final int slot;
  private final Command command;
}

@Data
class P2B implements Message {
  private final PaxosServer.Ballot ballot;
  private final int slot;
}

@Data
class Heartbeat implements Message {
  private final PaxosServer.Ballot ballot;
  private final int firstNonCleared;
  private final int lastNonEmpty;
  private final Map<Integer, LogEntry> chosenOrAccepted;
}

@Data
class HeartbeatReply implements Message {
  private final int slotOutMinusOne;
}
