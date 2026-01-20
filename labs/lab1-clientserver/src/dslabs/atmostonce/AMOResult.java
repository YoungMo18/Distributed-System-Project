package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
  // Your code here...
  private final Address clientId;
  private final int sequenceNum;
  private final Result result;
}
