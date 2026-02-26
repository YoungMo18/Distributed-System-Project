package dslabs.primarybackup;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  private final int sequenceNum; // the outstanding command's sequence number
}

@Data
final class ForwardRetryTimer implements Timer {
  static final int FORWARD_RETRY_MILLIS = 50;

  private final int viewNum; // the view number being forwarded to

  private final int clientSeqNum; // the client command's sequence number
}

@Data
final class StateTransferTimer implements Timer {
  static final int STATE_TRANSFER_RETRY_MILLIS = 50;

  private final int viewNum; // the view number being transferred to
}
