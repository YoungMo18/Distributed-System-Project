package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
}

/* -----------------------------------------------------------------------------------------------
 *  Client-Primary Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class ClientRequest implements Message {
  private final AMOCommand command;
}

@Data
class ServerReply implements Message {
  private final AMOResult result;
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class ForwardRequest implements Message {
  private final AMOCommand command;
  private final int viewNum;
}

@Data
class ForwardAck implements Message {
  private final boolean success;
  private final AMOCommand command;
  private final int viewNum;
}

/* -----------------------------------------------------------------------------------------------
 *  State Transfer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class StateTransferRequest implements Message {
  private final int viewNum;
  private final AMOApplication state;
}

@Data
class StateTransferAck implements Message {
  private final int viewNum;
}
