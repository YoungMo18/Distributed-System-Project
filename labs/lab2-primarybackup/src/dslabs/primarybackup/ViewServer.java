package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  private View currentView;
  private boolean pendingAck;
  private HashSet<Address> aliveNodes;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    this.currentView = new View(STARTUP_VIEWNUM, null, null); // init view
    this.pendingAck = false; // no primary to ack yet
    this.aliveNodes = new HashSet<>(); // no alive nodes yet
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {

    this.aliveNodes.add(sender); // first mark sender as alive

    if (currentView.viewNum() == STARTUP_VIEWNUM) { // first ping ever
      if (currentView.primary() == null) { // no primary yet
        this.currentView = new View(INITIAL_VIEWNUM, sender, null); // set primary
        this.pendingAck = true; // now waiting for primary ack
        this.send(new ViewReply(this.currentView), sender); // reply with current view
        return;
      }
    }

    // not startup case

    // current primary pings with correct view num
    if (sender.equals(currentView.primary()) && m.viewNum() == currentView.viewNum()) {
      this.pendingAck = false; // primary has acked

      if (currentView.backup() == null) { //
        // try to replace
        Address newBackup = getIdleServer();
        if (newBackup != null) {
          this.currentView =
              new View(currentView.viewNum() + 1, currentView.primary(), newBackup); // set backup
          this.pendingAck = true; // now waiting for primary ack
          this.send(new ViewReply(this.currentView), sender);
          return;
        }
      }
    }

    // normal case: no backup, idle server pinged
    if (!pendingAck
        && currentView.primary() != null
        && currentView.backup() == null
        && !sender.equals(currentView.primary())) {
      this.currentView =
          new View(currentView.viewNum() + 1, currentView.primary(), sender); // set backup
      this.pendingAck = true; // now waiting for primary ack
      this.send(new ViewReply(this.currentView), sender);
      return;
    }

    // always reply with current view
    this.send(new ViewReply(this.currentView), sender); // reply with current view
  }

  private void handleGetView(GetView m, Address sender) {
    this.send(new ViewReply(this.currentView), sender); // reply with current view
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    Address currentBackup = currentView.backup();
    Address currentPrimary = currentView.primary();

    // check if primary is dead
    if (isDead(currentPrimary)) {
      if (!this.pendingAck) { // only change view if primary has acked
        if (currentBackup != null) { // promote backup to primary
          Address newPrimary = currentBackup;
          Address newBackup = getIdleServer();
          this.currentView =
              new View(currentView.viewNum() + 1, newPrimary, newBackup); // change view
          this.pendingAck = true; // now waiting for new primary ack

        } else {
          // primary dead and no backup, gg
        }
      } else {
        // primary dead but hasn't acked yet, gg
      }

    } else { // primary alive
      if (isDead(currentBackup)) { // backup dead
        if (!this.pendingAck) { // primary acked
          Address newBackup = getIdleServer(); // find new backup
          if (newBackup != null
              || currentBackup
                  != null) { // only change view if there is a new backup or there was a backup
            // before
            this.currentView = new View(currentView.viewNum() + 1, currentPrimary, newBackup);
            this.pendingAck = true; // now waiting for primary to ack this new view
          }
        }
      }
    }

    this.aliveNodes.clear(); // reset alive nodes for next round
    this.set(new PingCheckTimer(), PING_CHECK_MILLIS); // reset timer
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // find an idle server
  public Address getIdleServer() {
    for (Address addr : this.aliveNodes) {
      if (!addr.equals(currentView.primary()) && !addr.equals(currentView.backup())) {
        return addr; // found idle server
      }
    }
    return null; // no idle server found
  }

  // if server is dead
  public boolean isDead(Address addr) {
    return !this.aliveNodes.contains(addr);
  }
}
