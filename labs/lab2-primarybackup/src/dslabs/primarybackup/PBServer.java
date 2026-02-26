package dslabs.primarybackup;

import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;

  private View currentView;
  private AMOApplication app;
  private boolean pendingAck;
  private boolean inStateTransfer;
  private AMOCommand lastForwardedCommand;
  private Address lastClientAddress;
  private int lastStateTransferViewNum;
  private int pingViewNum;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;
    this.app = new AMOApplication(app);
  }

  @Override
  public void init() {
    this.currentView = new View(STARTUP_VIEWNUM, null, null);
    this.pendingAck = false;
    this.inStateTransfer = false;
    this.lastForwardedCommand = null;
    this.lastClientAddress = null;
    this.lastStateTransferViewNum = STARTUP_VIEWNUM;
    this.pingViewNum = STARTUP_VIEWNUM;

    // ping view server immediately and start the PingTimer
    this.send(new Ping(this.currentView.viewNum()), this.viewServer);
    this.set(new PingTimer(), PingTimer.PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  // for primary to handle ClientRequest from client
  private void handleClientRequest(ClientRequest m, Address sender) {
    if (!this.address().equals(this.currentView.primary())) {
      return; // not primary
    }
    if (this.inStateTransfer) {
      return; // ignore while state transferring
    }
    if (this.pendingAck) {
      return; // already have one outstanding forwarded request; let client retry
    }

    this.lastForwardedCommand = m.command();
    this.lastClientAddress = sender;

    // forward to backup (if it exists)
    if (this.currentView.backup() != null) {
      this.send(
          new ForwardRequest(m.command(), this.currentView.viewNum()), this.currentView.backup());
      this.pendingAck = true;

      this.set(
          new ForwardRetryTimer(this.currentView.viewNum(), m.command().sequenceNum()),
          ForwardRetryTimer.FORWARD_RETRY_MILLIS);
      return;
    }

    // no backup: execute directly
    AMOResult result = app.execute(m.command());
    this.send(new ServerReply(result), sender);

    // clear outstanding bookkeeping
    this.lastForwardedCommand = null;
    this.lastClientAddress = null;
  }

  // for backup to handle ForwardRequest from primary
  private void handleForwardRequest(ForwardRequest m, Address sender) {
    if (!this.address().equals(this.currentView.backup())) {
      return; // not backup
    }

    if (this.inStateTransfer) {
      return;
    }

    if (!sender.equals(this.currentView.primary())) {
      return; // only accept from current primary
    }

    if (m.viewNum() != this.currentView.viewNum()) {
      return;
    }

    // execute on backup
    app.execute(m.command());

    // ack to primary
    this.send(new ForwardAck(true, m.command(), this.currentView.viewNum()), sender);
  }

  // for primary to handle ForwardAck from backup
  private void handleForwardAck(ForwardAck m, Address sender) {
    if (!this.address().equals(this.currentView.primary()) || !this.pendingAck) {
      return;
    }

    if (this.inStateTransfer) {
      return;
    }

    if (!sender.equals(this.currentView.backup())) {
      return;
    }

    if (m.viewNum() != this.currentView.viewNum()) {
      return;
    }

    // MUST match the outstanding forwarded request (avoid stale/delayed ack)
    if (this.lastForwardedCommand == null) {
      return;
    }
    if (!m.command().equals(this.lastForwardedCommand)) {
      return;
    }

    // done waiting
    this.pendingAck = false;

    // execute on primary AFTER backup ack (linearizable replication)
    AMOResult result = app.execute(this.lastForwardedCommand);

    // reply to the client who sent the request
    if (this.lastClientAddress != null) {
      this.send(new ServerReply(result), this.lastClientAddress);
    }

    // clear outstanding bookkeeping
    this.lastForwardedCommand = null;
    this.lastClientAddress = null;
  }

  private void handleViewReply(ViewReply m, Address sender) {
    if (!sender.equals(this.viewServer)) {
      return;
    }

    //  do NOT ignore ViewReply just because pendingAck is true.
    // view changes must be processed even while waiting for ForwardAck.

    if (this.currentView != null && m.view().viewNum() <= this.currentView.viewNum()) {
      return; // not newer
    }

    // accept the newer view and update immediately
    View oldView = this.currentView;
    this.currentView = m.view();

    this.pingViewNum = this.currentView.viewNum();

    Address oldPrimary = null;
    Address oldBackup = null;

    if (oldView != null) {
      oldPrimary = oldView.primary();
      oldBackup = oldView.backup();
    }

    Address newPrimary = this.currentView.primary();
    Address newBackup = this.currentView.backup();

    boolean iWasPrimary = (oldPrimary != null && this.address().equals(oldPrimary));
    boolean iWasBackup = (oldBackup != null && this.address().equals(oldBackup));

    boolean iAmPrimary = (newPrimary != null && this.address().equals(newPrimary));
    boolean iAmBackup = (newBackup != null && this.address().equals(newBackup));

    // If I am NOT the primary in the new view, I must not keep primary-only in-flight state
    if (!iAmPrimary) {
      this.pendingAck = false;
      this.lastForwardedCommand = null;
      this.lastClientAddress = null;
    }

    // If I just became the backup in a new view, I should wait for state transfer
    if (iAmBackup && !iWasBackup) {
      this.inStateTransfer = (this.lastStateTransferViewNum < this.currentView.viewNum());
      // (Primary will send StateTransferRequest; we’ll install it when it arrives)
    }

    // If I am primary and the backup changed/appeared, initiate state transfer
    if (iAmPrimary) {
      // If there is a backup and it is not the same as before, transfer state
      boolean backupChanged =
          (newBackup != null && (oldBackup == null || !newBackup.equals(oldBackup)));

      if (backupChanged) {
        this.inStateTransfer = true;

        this.pingViewNum = oldView.viewNum();

        this.send(new StateTransferRequest(this.currentView.viewNum(), this.app), newBackup);
        this.set(
            new StateTransferTimer(this.currentView.viewNum()),
            StateTransferTimer.STATE_TRANSFER_RETRY_MILLIS);
      }

      // if we were waiting for a ForwardAck and the view changed (e.g., backup died),
      // re-drive replication to the new backup (or execute if no backup).
      if (!this.inStateTransfer && this.pendingAck && this.lastForwardedCommand != null) {
        if (newBackup == null) {
          // no backup anymore: execute and reply (client must not hang)
          this.pendingAck = false;
          AMOResult result = app.execute(this.lastForwardedCommand);
          if (this.lastClientAddress != null) {
            this.send(new ServerReply(result), this.lastClientAddress);
          }
          this.lastForwardedCommand = null;
          this.lastClientAddress = null;
        } else {
          // re-forward to the new backup
          this.send(
              new ForwardRequest(this.lastForwardedCommand, this.currentView.viewNum()), newBackup);
          this.set(
              new ForwardRetryTimer(
                  this.currentView.viewNum(), this.lastForwardedCommand.sequenceNum()),
              ForwardRetryTimer.FORWARD_RETRY_MILLIS);
        }
      }
    }

    // If I'm idle now, clear state-transfer flag
    if (!iAmPrimary && !iAmBackup) {
      this.inStateTransfer = false; // wait for transfer
    }
  }

  // for backup to handle StateTransferRequest from primary
  private void handleStateTransferRequest(StateTransferRequest m, Address sender) {
    // if we know who the primary is, only accept from it.
    if (this.currentView.primary() != null && !sender.equals(this.currentView.primary())) {
      return;
    }

    // drop truly old transfers (older than what we've already installed)
    if (m.viewNum() < this.lastStateTransferViewNum) {
      return;
    }

    // if we already installed this view's state, re-ack (idempotent)
    if (m.viewNum() == this.lastStateTransferViewNum) {
      this.send(new StateTransferAck(m.viewNum()), sender);
      return;
    }

    this.app = m.state();
    this.lastStateTransferViewNum = m.viewNum();
    this.inStateTransfer = false;

    // ack using transfer's view num
    this.send(new StateTransferAck(m.viewNum()), sender);
  }

  // for primary to handle StateTransferAck from backup
  private void handleStateTransferAck(StateTransferAck m, Address sender) {
    if (!this.address().equals(this.currentView.primary())) {
      return;
    }

    if (this.currentView.backup() == null || !sender.equals(this.currentView.backup())) {
      return;
    }

    if (m.viewNum() != this.currentView.viewNum()) {
      return;
    }

    this.inStateTransfer = false;
    this.pingViewNum = this.currentView.viewNum();

    resumePendingIfAny();
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    // send Ping to view server
    this.send(new Ping(this.pingViewNum), this.viewServer);

    // reset timer
    this.set(t, PingTimer.PING_MILLIS);
  }

  private void onForwardRetryTimer(ForwardRetryTimer t) {
    // if timer t is not for current view num or not pending ack, ignore
    if (t.viewNum() != this.currentView.viewNum() || !this.pendingAck) {
      return;
    }

    if (!this.address().equals(this.currentView.primary())) {
      return;
    }

    if (this.currentView.backup() == null) {
      return;
    }

    this.send(
        new ForwardRequest(this.lastForwardedCommand, this.currentView.viewNum()),
        this.currentView.backup());

    this.set(t, ForwardRetryTimer.FORWARD_RETRY_MILLIS);
  }

  private void onStateTransferTimer(StateTransferTimer t) {
    if (t.viewNum() != this.currentView.viewNum()) {
      return;
    }

    if (!this.address().equals(this.currentView.primary())) {
      return;
    }

    if (this.currentView.backup() == null) {
      return;
    }

    if (!this.inStateTransfer) {
      return;
    }

    StateTransferRequest streq = new StateTransferRequest(this.currentView.viewNum(), this.app);
    this.send(streq, this.currentView.backup());

    this.set(t, StateTransferTimer.STATE_TRANSFER_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  // redrive an outstanding client op after a view change / state transfer
  private void resumePendingIfAny() {
    if (!this.address().equals(this.currentView.primary())) {
      return;
    }
    if (this.inStateTransfer) {
      return;
    }
    if (!this.pendingAck) {
      return;
    }
    if (this.lastForwardedCommand == null) {
      return;
    }

    Address newBackup = this.currentView.backup();

    if (newBackup == null) {
      // no backup now: execute locally and reply so client doesn't hang.
      this.pendingAck = false;
      AMOResult result = app.execute(this.lastForwardedCommand);
      if (this.lastClientAddress != null) {
        this.send(new ServerReply(result), this.lastClientAddress);
      }
      this.lastForwardedCommand = null;
      this.lastClientAddress = null;
      return;
    }

    // backup exists: re-forward to the current backup.
    this.send(new ForwardRequest(this.lastForwardedCommand, this.currentView.viewNum()), newBackup);
    this.set(
        new ForwardRetryTimer(this.currentView.viewNum(), this.lastForwardedCommand.sequenceNum()),
        ForwardRetryTimer.FORWARD_RETRY_MILLIS);
  }
}
