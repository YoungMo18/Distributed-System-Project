package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  // Your code here...
  private final Address clientId;
  private final int sequenceNum;
  private final Command command;

  @Override
  public boolean readOnly() {
    return command.readOnly();
  }
}
