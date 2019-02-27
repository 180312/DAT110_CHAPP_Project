package no.hvl.dat110.messages;

public class ConnectMsg extends Message {
	
	public ConnectMsg (String user) {
		super(MessageType.CONNECT, user);
	}
	
}
