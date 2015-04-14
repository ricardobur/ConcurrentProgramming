package NeilsenMizunoDijkstraScholten;


/*
 * class Node
 * 
 * This class represents each node of the predefined tree. As it is commented
 * on the MainNode class, we have a File entered on the .dot model. All nodes
 * that aren't the MainNode are instances from these class. These class contains
 * all the different variables and functions to implement the two algorihtms and
 * the increment or blur function. 
 * 
 * Actually, the Node class has two parts, the main part, defined here, where it
 * is done all main work and the receive part where can be received the messages
 * sent on the queue to this node. 
 * 
 * We will define this as a Thread. We will have the main thread Node where it 
 * will be executed the send of messages and the different works and the receive
 * Thread formed by an instance of Receive class, where we will receive the 
 * different messages.
 * 
 * These two thread will be related between them with a BlockingQueue. So, when
 * the Node receive a Message if it is any kind of work it will be included on 
 * that Queue.
 */

import java.io.IOException;
import java.io.StringWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import Beanstalk.BeanstalkException;


/*
 * class Message
 * 
 * This class is used to create the different kind of messages to be sent through beanstalk
 * messages queue. According to the algorithms and the included functionalities we have 
 * these messages:
 * 
 * -> Increment: indicates that the node will have to increment till the given value
 * -> Blur: indicates that the node will have to blur the rows given
 * -> Token: send the token for the Neilsen-Mizuno Algorithm
 * -> Request: send a request to a node for the token, Neilsen-Mizuno algorithm
 * -> Signal: send a signal to indicate its works has ended
 * 
 * 
 * */

public class Message {
	/* Define a message of work, incremetn or blur image */
	public static final int ID_INCREMENT= 0;
	public static final int ID_BLUR = 1;
	
	/* In Neilsen - Mizuno we have two kinds of messages, the request and the token*/
	public static final int ID_REQUEST = 3;
	public static final int ID_TOKEN = 4;
	
	/* Signal Id to indicate the node has ended the job */
	public static final int ID_SIGNAL = 5;
	
	/* Variables to know the kind of message and the node who send it*/
	private int id;
	private Node node;
	
	public Message(int idMsg, Node sendNode){
		
		this.id = idMsg;
		this.node = sendNode;
		
	}
	
	/* Constructor for the message when we junt want to receive*/
	public Message(){
		
	}
	
	/*
	 *Method in charge of converting the value from JSON to String to send it 
	 * */
	public String incrementWork(int args) throws IOException, BeanstalkException{
		
		JSONObject obj = new JSONObject();
		
		/* Fill up the fields according to the type */
		obj.put("id", this.id);
		obj.put("nodo", this.node.getIdNode());
		obj.put("argumentos", args);
		
		/* Convert JSON to String */
		StringWriter stringMsg = new StringWriter();
	    obj.writeJSONString(stringMsg);
	    
	    String jsonText = stringMsg.toString(); 
	
	    return jsonText;
	}
	
	/*
	 * Method to convert from String to JSON again the values
	 * 
	 * */
	public JSONObject stringToJSON(String msg) throws ParseException{
		JSONObject jsonObj = (JSONObject)new JSONParser().parse(msg);
		return jsonObj;
	}
	
	
	/*
	 * Method in charge of building the JSON and code into an String to send it for Neilsen-Mizuno. To read the 
	 * JSON it can be used the same
	 * */
	public String jsonToStringNM(int source, int originator) throws IOException, BeanstalkException{
		
		JSONObject obj = new JSONObject();
			
		obj.put("id", this.id);
		obj.put("parent", this.node.parent);
		obj.put("token", 0); // request without token
		obj.put("source", source); // source from where we receive the token
		obj.put("originator", originator); // First who origin the request
		
		
		StringWriter stringMsg = new StringWriter();
	    obj.writeJSONString(stringMsg);
	    String jsonText = stringMsg.toString();
	
	    return jsonText;
	    
	}
	
	/*
	 * Method in charge of sending the token message
	 * */
	public String jsonToStringToken() throws IOException{
	
		JSONObject obj = new JSONObject();
		
		obj.put("id", this.id);
		obj.put("token", 1); // request with token
		
		StringWriter stringMsg = new StringWriter();
	    obj.writeJSONString(stringMsg);
	    String jsonText = stringMsg.toString();
	    
	    return jsonText;
	}
	
	
	/*
	 * Method in charge of generate the message to send the Signal for Dijkstra-Scholten
	 * */
	public String jsonToStringSignal(int idNode){
		
		JSONObject obj = new JSONObject();
		
		obj.put("id", this.id);
		obj.put("idNode", idNode);
		
		StringWriter stringMsg = new StringWriter();
		
	    try {
			obj.writeJSONString(stringMsg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    String jsonText = stringMsg.toString();
	
	    return jsonText;
	}
	
	
	/* Method in charge of generating the JSON with values
	 * 1) Message id
	 * 2) Id's Node who send the work
	 * 3) Pixels of the axis and the image
	 * 
	 * */
	public String blurWork(int yInit, int yEnd) throws IOException, BeanstalkException{
		
		JSONObject obj = new JSONObject();
		
		obj.put("id", this.id);
		obj.put("nodo", this.node.getIdNode());
		obj.put("inicio", yInit);
		obj.put("fin", yEnd);
		
		StringWriter stringMsg = new StringWriter();
	    obj.writeJSONString(stringMsg);
	    String jsonText = stringMsg.toString();
	    
	
	    return jsonText;
	    
	}
	

}
