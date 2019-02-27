package no.hvl.dat110.broker;

import java.util.Set;
import java.util.Collection;
import java.util.Queue;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

	private Storage storage;

	public Dispatcher(Storage storage) {
		super("Dispatcher");
		this.storage = storage;

	}

	@Override
	public void doProcess() {

		Collection<ClientSession> clients = storage.getSessions();

		Logger.lg(".");
		for (ClientSession client : clients) {

			Message msg = null;

			if (client.hasData()) {
				msg = client.receive();
			}

			if (msg != null) {
				dispatch(client, msg);
			}
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void dispatch(ClientSession client, Message msg) {

		MessageType type = msg.getType();

		switch (type) {

		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;

		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;

		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;

		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;

		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;

		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;

		default:
			Logger.log("broker dispatch - unhandled message type");
			break;

		}
	}

	public void onConnect(ConnectMsg msg, Connection connection) {

		String user = msg.getUser();
		
		storage.addClientSession(user, connection);
		
		Logger.log("onConnect:" + msg.toString());
		
		Queue<Message> messages = storage.getWaitingMessages(msg.getUser());
		
		if(messages != null) {
			messages.stream().forEach(message -> storage.getSession(msg.getUser()).send(message));
		}
		
		storage.resetMessageQueue(msg.getUser());
	}

	public void onDisconnect(DisconnectMsg msg) {

		String user = msg.getUser();
		
		storage.newMessageQueue(msg.getUser());

		Logger.log("onDisconnect:" + msg.toString());

		storage.removeClientSession(user);

	}

	public void onCreateTopic(CreateTopicMsg msg) {

		Logger.log("onCreateTopic:" + msg.toString());
		
		storage.createTopic(msg.getTopic());
	}

	public void onDeleteTopic(DeleteTopicMsg msg) {

		Logger.log("onDeleteTopic:" + msg.toString());

		storage.deleteTopic(msg.getTopic());
	}

	public void onSubscribe(SubscribeMsg msg) {

		Logger.log("onSubscribe:" + msg.toString());
		
		storage.addSubscriber(msg.getUser(), msg.getTopic());
		
	}

	public void onUnsubscribe(UnsubscribeMsg msg) {

		Logger.log("onUnsubscribe:" + msg.toString());
		
		storage.removeSubscriber(msg.getUser(), msg.getTopic());

	}

	public void onPublish(PublishMsg msg) {

		Logger.log("onPublish:" + msg.toString());
		
		storage.addMessageToQueue(msg);
		
		Set<String> subscribers = storage.getSubscribers(msg.getTopic());
		if(subscribers != null) {
			subscribers.stream().forEach(user -> {
				if(storage.getSession(user) != null) {
				storage.getSession(user).send(new PublishMsg(msg.getUser(), msg.getTopic(), msg.getMessage()));
				}
			});
		}
		
	}
}
