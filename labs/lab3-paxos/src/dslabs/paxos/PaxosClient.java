package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...
  private AMOCommand pendingCommand;
  private int sequenceNum;
  private Result lastResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
    this.pendingCommand = null;
    this.sequenceNum = 0;
    this.lastResult = null;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    // Your code here...
    this.sequenceNum++;
    this.pendingCommand = new AMOCommand(this.address(), this.sequenceNum, operation);
    this.lastResult = null;

    broadcastRequest(this.pendingCommand);
    this.set(new ClientTimer(this.sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return this.lastResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (this.lastResult == null) {
      this.wait();
    }
    return this.lastResult;
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    // Your code here...
    AMOResult result = m.result();
    if (this.pendingCommand == null) {
      return;
    }
    if (!this.address().equals(result.clientId())) {
      return;
    }
    if (result.sequenceNum() != this.sequenceNum) {
      return;
    }

    this.lastResult = result.result();
    this.pendingCommand = null;
    this.notify();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (this.pendingCommand == null) {
      return;
    }
    if (t.sequenceNum() != this.sequenceNum) {
      return;
    }

    broadcastRequest(this.pendingCommand);
    this.set(t, ClientTimer.CLIENT_RETRY_MILLIS);
  }

  private void broadcastRequest(AMOCommand command) {
    PaxosRequest request = new PaxosRequest(command);
    for (Address server : servers) {
      this.send(request, server);
    }
  }
}
