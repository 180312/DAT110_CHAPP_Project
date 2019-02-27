package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;

public class DisplayDevice {
	
	private static final int COUNT = 10;
		
	public static void main (String[] args) {
		
		System.out.println("Display starting ...");
		
		// TODO - START
		
		Client client = new Client("display", Common.BROKERHOST, Common.BROKERPORT);
		
		client.connect();
		
		client.createTopic(Common.TEMPTOPIC);
		
		client.subscribe(Common.TEMPTOPIC);
		
		for (int i = 0; i<COUNT; i++) {
			
			PublishMsg message = (PublishMsg) client.receive();
			
			System.out.println("Device is displaying: " + message.getMessage());
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		client.disconnect();
				
		// TODO - END
		
		System.out.println("Display stopping ... ");
		
	}
}
