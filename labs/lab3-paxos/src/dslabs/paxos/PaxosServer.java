package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  // Your code here...
  private final AMOApplication<Application> app;

  private final Map<Integer, LogEntry> log = new HashMap<>();
  private final Map<Integer, LogEntry> tempLog = new HashMap<>();

  private Ballot currentBallot;
  private Ballot promisedBallot;
  private Ballot knownLeaderBallot;
  private boolean isActiveLeader;

  private final Set<Address> p1bResponses = new HashSet<>();
  private final Map<Integer, Set<Address>> p2bResponses = new HashMap<>();

  private int slotOut;  // index of next slot to execute
  private int nextSlotIn;  // index of next slot to propose new command in
  private int firstNonCleared;  // lowest slot that hasnt been GCed
  private int lastNonEmpty;  // highest slot that has been accepted or chosen

  private final Map<Address, Integer> executedUpTo = new HashMap<>();  // for GC tracking

  private int consecutiveHeartbeatMisses;

  private final Map<Integer, Address> pendingClientBySlot = new HashMap<>();

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    // Your code here...
    this.app = new AMOApplication<>(app);
  }

  @Override
  public void init() {
    // Your code here...
    Ballot initBallot = new Ballot(0, this.address());
    this.currentBallot = initBallot;
    this.promisedBallot = initBallot;
    this.knownLeaderBallot = null;
    this.isActiveLeader = false;

    this.slotOut = 1;
    this.nextSlotIn = 1;
    this.firstNonCleared = 1;
    this.lastNonEmpty = 0;

    this.consecutiveHeartbeatMisses = 0;

    for (Address server : servers) {
      this.executedUpTo.put(server, 0);  // so far no slots have been executed at any server
    }
    this.executedUpTo.put(this.address(), 0);

    this.set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);

    if (servers.length == 1) {  // rare: we are the only server
      becomeLeaderNow();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    if (logSlotNum < this.firstNonCleared) {
      return PaxosLogSlotStatus.CLEARED;
    }
    if (logSlotNum > this.lastNonEmpty) {
      return PaxosLogSlotStatus.EMPTY;
    }

    LogEntry e = this.log.get(logSlotNum);
    if (e == null) {
      return PaxosLogSlotStatus.EMPTY;
    }
    return e.status();
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    PaxosLogSlotStatus s = status(logSlotNum);
    if (s == PaxosLogSlotStatus.CLEARED || s == PaxosLogSlotStatus.EMPTY) {
      return null;
    }

    LogEntry e = this.log.get(logSlotNum);
    if (e == null || e.command() == null) {
      return null;
    }

    Command c = e.command();
    if (c instanceof AMOCommand) {
      return ((AMOCommand) c).command();
    }
    return c;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    return this.firstNonCleared;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    return Math.max(this.lastNonEmpty, this.firstNonCleared - 1);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    if (!this.isActiveLeader) {
      return;
    }

    AMOCommand command = m.command();
    if (command == null) {
      return;
    }

    if (this.app.alreadyExecuted(command)) {
      AMOResult cached = this.app.execute(command);
      this.send(new PaxosReply(cached), sender);
      return;
    }

    Integer existingSlot = findExistingSlot(command);
    if (existingSlot != null) {
      this.pendingClientBySlot.putIfAbsent(existingSlot, sender);
      LogEntry e = this.log.get(existingSlot);
      if (e != null && e.status() == PaxosLogSlotStatus.ACCEPTED) {
        reproposeAccepted(existingSlot, e.command());
      }
      executeChosenSlots();
      return;
    }

    int slot = allocateNextSlot();
    this.pendingClientBySlot.put(slot, sender);
    reproposeAccepted(slot, command);
    executeChosenSlots();
  }

  // Your code here...
  // handles phase 1A from a candidate leader
  private void handleP1A(P1A m, Address sender) {
    if (m.ballot().compareTo(this.promisedBallot) <= 0) {
      return;
    }

    observeHigherBallot(m.ballot());  // some higher ballot is found, step down
    this.promisedBallot = m.ballot();
    this.knownLeaderBallot = maxBallot(this.knownLeaderBallot, m.ballot());

    this.send(new P1B(m.ballot(), snapshotLog()), sender);  // respond with p1b
  }

  // collects phase 1B from majority to become leader
  private void handleP1B(P1B m, Address sender) {
    if (this.isActiveLeader) {
      return;
    }

    if (!m.ballot().equals(this.currentBallot)) {
      return;
    }

    if (!this.p1bResponses.add(sender)) {
      return;
    }

    mergeSnapshotInto(this.tempLog, m.logSnapshot());  // merge incoming logs into templog

    if (!hasMajority(this.p1bResponses.size())) {
      return;  // dont clear templog here, still possible to receive majority p1bs
    }

    // achieved majority at this point, merge templog into log and become leader
    mergeSnapshotInto(this.log, this.tempLog);
    this.lastNonEmpty = Math.max(this.lastNonEmpty, highestSlotIn(this.log));
    this.tempLog.clear();

    int high = highestKnownSlot();
    for (int i = this.firstNonCleared; i <= high; i++) {  // noop holes
      if (!this.log.containsKey(i)) {
        this.log.put(
            i, new LogEntry(this.currentBallot, NoOpCommand.INSTANCE, PaxosLogSlotStatus.ACCEPTED));
        this.lastNonEmpty = Math.max(this.lastNonEmpty, i);
      }
    }

    this.nextSlotIn = Math.max(this.nextSlotIn, this.lastNonEmpty + 1);
    becomeLeaderNow();

    for (int slot = this.firstNonCleared; slot <= this.lastNonEmpty; slot++) {
      LogEntry e = this.log.get(slot);
      if (e != null && e.status() == PaxosLogSlotStatus.ACCEPTED) {
        reproposeAccepted(slot, e.command());
      }
    }  // repropoe any accepted commands (optimization)

    executeChosenSlots();  // execute any already chosen commands
  }

  // handles pphase 2A from the leader
  private void handleP2A(P2A m, Address sender) {
    if (m.ballot().compareTo(this.promisedBallot) < 0) {
      return;
    }

    // ballot updates if needed
    observeHigherBallot(m.ballot());
    this.promisedBallot = m.ballot();

    LogEntry existing = this.log.get(m.slot());
    if (existing == null || existing.status() != PaxosLogSlotStatus.CHOSEN) {  // not chosen yet, can accept
      this.log.put(m.slot(), new LogEntry(m.ballot(), m.command(), PaxosLogSlotStatus.ACCEPTED));
      this.lastNonEmpty = Math.max(this.lastNonEmpty, m.slot());
      this.nextSlotIn = Math.max(this.nextSlotIn, m.slot() + 1);
    }

    this.send(new P2B(m.ballot(), m.slot()), sender);  // respond with p2b
  }

  // handles phase 2B responses as the leader
  private void handleP2B(P2B m, Address sender) {
    if (!this.isActiveLeader) {
      return;
    }

    if (!m.ballot().equals(this.currentBallot)) {
      return;
    }

    Set<Address> responses = this.p2bResponses.computeIfAbsent(m.slot(), k -> new HashSet<>());
    responses.add(sender);

    if (!hasMajority(responses.size())) {
      return;  // waiting on possible majority, dont mark chosen until then
    }

    // majority achieved, mark chosen if not already and execute
    LogEntry e = this.log.get(m.slot());
    if (e == null || e.status() == PaxosLogSlotStatus.CHOSEN) {
      return;
    }

    this.log.put(
        m.slot(), new LogEntry(this.currentBallot, e.command(), PaxosLogSlotStatus.CHOSEN));
    this.p2bResponses.remove(m.slot());

    executeChosenSlots();  // execute any newly chosen commands
  }

  // handles leader heartbeats
  private void handleHeartbeat(Heartbeat m, Address sender) {
    if (m.ballot().compareTo(this.promisedBallot) < 0) {
      this.send(new HeartbeatReply(this.slotOut - 1), sender);
      return;
    }  // slotOut - 1 is the last slot we have executed

    observeHigherBallot(m.ballot());  // update ballot and step down if needed
    this.promisedBallot = m.ballot();
    this.knownLeaderBallot = maxBallot(this.knownLeaderBallot, m.ballot());
    this.consecutiveHeartbeatMisses = 0;

    mergeSnapshotInto(this.log, m.chosenOrAccepted());  // merge incoming log entries into local log
    this.lastNonEmpty = Math.max(this.lastNonEmpty, highestSlotIn(this.log));
    this.lastNonEmpty = Math.max(this.lastNonEmpty, m.lastNonEmpty());
    this.nextSlotIn = Math.max(this.nextSlotIn, this.lastNonEmpty + 1);

    if (m.firstNonCleared() > this.firstNonCleared) {
      clearUpTo(m.firstNonCleared() - 1);
    }

    executeChosenSlots();

    this.send(new HeartbeatReply(this.slotOut - 1), sender);  // reply 
  }

  // handles follower progress replies at the leader
  private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
    if (!this.isActiveLeader) {
      return;
    }

    this.executedUpTo.put(
        sender, Math.max(this.executedUpTo.getOrDefault(sender, 0), m.slotOutMinusOne()));
    this.executedUpTo.put(
        this.address(),
        Math.max(this.executedUpTo.getOrDefault(this.address(), 0), this.slotOut - 1));

    int minExecuted = Integer.MAX_VALUE;  // max boundary 
    for (Address server : servers) {
      minExecuted = Math.min(minExecuted, this.executedUpTo.getOrDefault(server, 0));
    }

    if (minExecuted >= this.firstNonCleared) {
      clearUpTo(minExecuted);  // actualt clear log slots, update lastNonEmpty and firstNonCleared
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  // broadcasts heartbeats to maintain leadership
  private void onHeartbeatTimer(HeartbeatTimer t) {
    if (!this.isActiveLeader) {
      return;
    }

    sendHeartbeat();
    this.set(t, HeartbeatTimer.HEARTBEAT_MILLIS);
  }

  // if we miss >= 2 leader heartbeats, start a new election
  private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
    if (this.isActiveLeader) {
      this.set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
      return;
    }

    this.consecutiveHeartbeatMisses++;
    if (this.consecutiveHeartbeatMisses >= 2) {
      startElection();
      this.consecutiveHeartbeatMisses = 0;
    }

    this.set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
  }

  // retransmits phase 1A (prepare) while campaigning until leadership is obtained or ballot changes
  private void onElectionRetryTimer(ElectionRetryTimer t) {
    if (this.isActiveLeader) {
      return;
    }
    if (!t.ballot().equals(this.currentBallot)) {
      return;
    }

    broadcastExceptSelf(new P1A(this.currentBallot));
    this.set(t, ElectionRetryTimer.ELECTION_RETRY_MILLIS);
  }

  // retransmits phase 2A (accept) for a slot while it is not yet CHOSEN to handle message loss
  private void onProposalRetryTimer(ProposalRetryTimer t) {
    if (!this.isActiveLeader) {
      return;
    }
    if (!t.ballot().equals(this.currentBallot)) {
      return;
    }

    LogEntry e = this.log.get(t.slot());
    if (e == null || e.status() == PaxosLogSlotStatus.CHOSEN) {
      return;
    }

    broadcastExceptSelf(new P2A(this.currentBallot, t.slot(), e.command()));
    this.set(t, ProposalRetryTimer.PROPOSAL_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  // starts new leader election by choosing a ballot higher than any seen ballot
  private void startElection() {
    int max = this.currentBallot.number();
    max = Math.max(max, this.promisedBallot.number());
    if (this.knownLeaderBallot != null) {
      max = Math.max(max, this.knownLeaderBallot.number());
    }

    this.currentBallot = new Ballot(max + 1, this.address());
    this.promisedBallot = this.currentBallot;
    this.isActiveLeader = false;

    this.p1bResponses.clear();
    this.tempLog.clear();

    this.p1bResponses.add(this.address());
    mergeSnapshotInto(this.tempLog, snapshotLog());

    broadcastExceptSelf(new P1A(this.currentBallot));
    this.set(new ElectionRetryTimer(this.currentBallot), ElectionRetryTimer.ELECTION_RETRY_MILLIS);

    if (hasMajority(this.p1bResponses.size())) {
      mergeSnapshotInto(this.log, this.tempLog);
      this.lastNonEmpty = Math.max(this.lastNonEmpty, highestSlotIn(this.log));
      this.tempLog.clear();
      becomeLeaderNow();
    }
  }

  // transitions into active leader state for currentBallot
  private void becomeLeaderNow() {
    this.isActiveLeader = true;
    this.knownLeaderBallot = this.currentBallot;
    this.consecutiveHeartbeatMisses = 0;

    this.p1bResponses.clear();
    this.tempLog.clear();

    this.executedUpTo.put(this.address(), this.slotOut - 1);

    sendHeartbeat();
    this.set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
  }

  // executes CHOSEN log entries strictly in increasing slot order starting at slotOut
  private void executeChosenSlots() {
    while (true) {
      LogEntry e = this.log.get(this.slotOut);
      if (e == null || e.status() != PaxosLogSlotStatus.CHOSEN) {
        break;
      }

      Command c = e.command();
      Address pendingClient = this.pendingClientBySlot.remove(this.slotOut);

      if (c instanceof AMOCommand) {
        AMOResult r = this.app.execute(c);
        if (this.isActiveLeader && pendingClient != null) {
          this.send(new PaxosReply(r), pendingClient);
        }
      }

      this.p2bResponses.remove(this.slotOut);
      this.slotOut++;
    }

    this.executedUpTo.put(this.address(), this.slotOut - 1);
  }

  // searches the current log for a slot that already contains this command
  private Integer findExistingSlot(AMOCommand command) {
    for (int slot = this.firstNonCleared; slot <= this.lastNonEmpty; slot++) {
      LogEntry e = this.log.get(slot);
      if (e == null) {
        continue;
      }
      if (e.status() == PaxosLogSlotStatus.EMPTY || e.status() == PaxosLogSlotStatus.CLEARED) {
        continue;
      }
      if (command.equals(e.command())) {  // needs hashcode() to be defined
        return slot;
      }
    }
    return null;
  }

  // picks the next available EMPTY slot to place new proposal starting from nextSlotIn
  private int allocateNextSlot() {
    int s = Math.max(this.nextSlotIn, this.firstNonCleared);
    while (true) {
      LogEntry e = this.log.get(s);
      if (e == null || e.status() == PaxosLogSlotStatus.EMPTY) {
        break;
      }
      s++;
    }
    this.nextSlotIn = s + 1;
    return s;
  }

  // proposes a value for a slot by sending P2A messages and tracking acks
  private void reproposeAccepted(int slot, Command command) {
    this.log.put(slot, new LogEntry(this.currentBallot, command, PaxosLogSlotStatus.ACCEPTED));
    this.lastNonEmpty = Math.max(this.lastNonEmpty, slot);

    Set<Address> responses = new HashSet<>();
    responses.add(this.address());
    this.p2bResponses.put(slot, responses);

    broadcastExceptSelf(new P2A(this.currentBallot, slot, command));
    this.set(
        new ProposalRetryTimer(this.currentBallot, slot), ProposalRetryTimer.PROPOSAL_RETRY_MILLIS);

    if (hasMajority(responses.size())) {
      this.log.put(slot, new LogEntry(this.currentBallot, command, PaxosLogSlotStatus.CHOSEN));
      this.p2bResponses.remove(slot);
    }
  }

  // sends a heartbeat to followers with current ballot and log state
  private void sendHeartbeat() {
    this.executedUpTo.put(this.address(), this.slotOut - 1);

    Map<Integer, LogEntry> snapshot = snapshotLog();
    Heartbeat hb =
        new Heartbeat(this.currentBallot, this.firstNonCleared, this.lastNonEmpty, snapshot);

    broadcastExceptSelf(hb);
  }

  // clears log entries up to the given slot and updates lastNonEmpty
  private void clearUpTo(int slotInclusive) {
    if (slotInclusive < this.firstNonCleared) {
      return;
    }

    for (int slot = this.firstNonCleared; slot <= slotInclusive; slot++) {
      this.log.remove(slot);
      this.tempLog.remove(slot);
      this.p2bResponses.remove(slot);
      this.pendingClientBySlot.remove(slot);
    }

    this.firstNonCleared = slotInclusive + 1;
    this.lastNonEmpty = Math.max(this.lastNonEmpty, this.firstNonCleared - 1);
    if (this.slotOut < this.firstNonCleared) {
      this.slotOut = this.firstNonCleared;
    }
  }

  // updates our ballot state and steps down if a higher ballot is seen
  private void observeHigherBallot(Ballot ballot) {
    if (ballot.compareTo(this.currentBallot) > 0) {
      this.currentBallot = ballot;
      this.isActiveLeader = false;
    }
    this.knownLeaderBallot = maxBallot(this.knownLeaderBallot, ballot);
  }

  // sends a message to all Paxos peers except this node
  private void broadcastExceptSelf(dslabs.framework.Message m) {
    for (Address server : servers) {
      if (server.equals(this.address())) {
        continue;
      }
      this.send(m, server);
    }
  }

  // check if majority is achieved (more than half)
  private boolean hasMajority(int count) {
    return 2 * count > this.servers.length;
  }

  // creates a copy of the local log for election/heartbeat exchange
  private Map<Integer, LogEntry> snapshotLog() {
    Map<Integer, LogEntry> snapshot = new HashMap<>();
    for (Map.Entry<Integer, LogEntry> e : this.log.entrySet()) {
      int slot = e.getKey();
      if (slot < this.firstNonCleared) {
        continue;
      }
      snapshot.put(slot, copyEntry(e.getValue()));
    }
    return snapshot;
  }

  // merges log entries preferring CHOSEN entries and otherwise the higher ballot
  private void mergeSnapshotInto(Map<Integer, LogEntry> target, Map<Integer, LogEntry> src) {
    for (Map.Entry<Integer, LogEntry> e : src.entrySet()) {
      int slot = e.getKey();
      if (slot < this.firstNonCleared) {
        continue;
      }

      LogEntry incoming = copyEntry(e.getValue());
      LogEntry existing = target.get(slot);

      if (existing == null) {
        target.put(slot, incoming);
      } else if (shouldReplace(existing, incoming)) {
        target.put(slot, incoming);
      }
    }
  }

  // check if the incoming entry should replace the existing one
  private static boolean shouldReplace(LogEntry existing, LogEntry incoming) {
    if (existing.status() == PaxosLogSlotStatus.CHOSEN
        && incoming.status() != PaxosLogSlotStatus.CHOSEN) {
      return false;
    }
    if (incoming.status() == PaxosLogSlotStatus.CHOSEN
        && existing.status() != PaxosLogSlotStatus.CHOSEN) {
      return true;
    }

    if (incoming.ballot() == null) {
      return false;
    }
    if (existing.ballot() == null) {
      return true;
    }

    return incoming.ballot().compareTo(existing.ballot()) > 0;
  }

  // creates a shallow copy of a log entry
  private static LogEntry copyEntry(LogEntry e) {
    if (e == null) {
      return null;
    }
    return new LogEntry(e.ballot(), e.command(), e.status());
  }

  // returns the highest slot index known locally
  private int highestKnownSlot() {
    int high = this.lastNonEmpty;
    for (Integer slot : this.log.keySet()) {
      high = Math.max(high, slot);
    }
    return high;
  }

  // returns the max slot index present in a given log map
  private static int highestSlotIn(Map<Integer, LogEntry> entries) {
    int high = 0;
    for (Integer slot : entries.keySet()) {
      high = Math.max(high, slot);
    }
    return high;
  }

  // returns the higher ballot
  private static Ballot maxBallot(Ballot a, Ballot b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.compareTo(b) >= 0 ? a : b;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Types
   * ---------------------------------------------------------------------------------------------*/
  // class for ballot
  static final class Ballot implements Comparable<Ballot>, Serializable {
    private final int number;  // round number
    private final Address leaderId;  // leader ID is just its address 

    Ballot(int number, Address leaderId) {
      this.number = number;
      this.leaderId = leaderId;
    }

    int number() {
      return number;
    }

    Address leaderId() {
      return leaderId;
    }

    @Override
    public int compareTo(Ballot o) {
      int byNumber = Integer.compare(this.number, o.number);
      if (byNumber != 0) {
        return byNumber;
      }

      return this.leaderId.compareTo(o.leaderId);  // tiebreak with leader ID
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Ballot)) {
        return false;
      }

      Ballot ballot = (Ballot) o;
      return number == ballot.number && Objects.equals(leaderId, ballot.leaderId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(number, leaderId);
    }
  }

  // class for logentry
  @Data
  static final class LogEntry implements Serializable {
    private final Ballot ballot;
    private final Command command;
    private final PaxosLogSlotStatus status;
  }

  // class for noop
  static final class NoOpCommand implements Command {
    static final NoOpCommand INSTANCE = new NoOpCommand();  // singleton since all no-ops are equivalent

    private NoOpCommand() {}

    @Override
    public boolean readOnly() {  // default is false, so we can ovverdie that 
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NoOpCommand;
    }

    @Override
    public int hashCode() {  // need this for equals()
      return 67;
    }
  }
}
