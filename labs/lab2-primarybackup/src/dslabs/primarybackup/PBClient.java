package dslabs.primarybackup;

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
class PBClient extends Node implements Client {
  private final Address viewServer;

  private View currentView;
  private AMOCommand pendingCommand;
  private int sequenceNum;
  private Result lastResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
  }

  @Override
  public synchronized void init() {
    this.currentView = null;
    this.pendingCommand = null;
    this.sequenceNum = 0;
    this.lastResult = null;

    // send initial GetView to view server
    this.send(new GetView(), this.viewServer);

    // set PingTimer

  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    this.sequenceNum++;
    this.pendingCommand = new AMOCommand(this.address(), this.sequenceNum, command);
    this.lastResult = null;

    // current primary could be null if no view yet (on init)
    if (currentView == null || currentView.primary() == null) {
      // send a GetView to view server to get current view
      this.send(new GetView(), this.viewServer);
      this.set(new ClientTimer(this.sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
      return; // will send command when ClientTimer fires
    }

    // view is not null and current primary != null, safe to send command
    this.send(new ClientRequest(this.pendingCommand), currentView.primary());

    this.set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return this.lastResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    while (this.lastResult == null) {
      wait();
    }
    return this.lastResult;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleServerReply(ServerReply m, Address sender) {

    AMOResult result = m.result();
    if (!this.address().equals(result.clientId())
        || // this result not for me
        result.sequenceNum() != this.sequenceNum) { // or stale result for me
      return; // stale reply
    }
    this.lastResult = result.result(); // store result
    this.pendingCommand = null; // clear pending command
    this.notify(); // wake up getResult()
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {

    if (!sender.equals(this.viewServer)) {
      return; // ignore if not from view server
    }

    if (this.currentView == null
        || // if no current view
        m.view().viewNum() > this.currentView.viewNum()) { // or received view is newer
      this.currentView = m.view(); // update current view
    }
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // if timer t is not for current sequence num or command not pending, ignore
    if (t.sequenceNum() != this.sequenceNum || this.pendingCommand == null) {
      return; // stale timer or command already completed
    }

    // check if current view is null or primary is null
    if (this.currentView == null || this.currentView.primary() == null) {
      // don't do anything
    } else {
      // resend pending command to primary (could be new or old primary)
      this.send(new ClientRequest(this.pendingCommand), currentView.primary());
    }

    // send a GetView to view server to make sure our current view is up to date
    this.send(new GetView(), this.viewServer);

    // reset ClientTimer
    this.set(t, ClientTimer.CLIENT_RETRY_MILLIS);
  }
}
