package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;

  // Your code here...
  private AMOCommand currentCommand = null;
  private Result currentResult = null;
  private int sequenceNum = 0;
  private boolean waiting = false;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.serverAddress = serverAddress;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    sequenceNum++;
    currentCommand = new AMOCommand(this.address(), sequenceNum, command);
    currentResult = null;
    waiting = true;

    this.send(new Request(currentCommand), serverAddress);
    this.set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...

    return currentResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (currentResult == null) {
      this.wait();
    }

    return currentResult;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    // only accept reply for the current outstanding request
    AMOResult r = m.result();
    if (!this.address().equals(r.clientId()) || r.sequenceNum() != sequenceNum) {
      return;
    }
    currentResult = r.result();
    waiting = false;
    this.notify();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * -----------------------------run-tests.py --lab 1 --part 2----------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    // discard old timer
    if (!waiting || t.sequenceNum() != sequenceNum) {
      return;
    }

    // retry same request
    send(new Request(currentCommand), serverAddress);
    set(t, ClientTimer.CLIENT_RETRY_MILLIS);
  }
}
