package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;

  // Your code here...
  private final HashMap<Address, AMOResult> lastResult = new HashMap<>();

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    // Your code here...
    AMOResult cached = lastResult.get(amoCommand.clientId());

    if (cached != null && amoCommand.sequenceNum() <= cached.sequenceNum()) {
      return cached;
    }

    Result r = application.execute(amoCommand.command());
    AMOResult newResult = new AMOResult(amoCommand.clientId(), amoCommand.sequenceNum(), r);
    lastResult.put(amoCommand.clientId(), newResult);
    return newResult;
  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    AMOResult cached = lastResult.get(amoCommand.clientId());
    return cached != null && amoCommand.sequenceNum() <= cached.sequenceNum();
  }
}
