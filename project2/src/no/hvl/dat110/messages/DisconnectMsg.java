package no.hvl.dat110.messages;

public class DisconnectMsg extends Message {
	
	public DisconnectMsg(String user) {
		super(MessageType.DISCONNECT, user);
	}
	
}
