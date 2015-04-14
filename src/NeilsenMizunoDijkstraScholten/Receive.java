package NeilsenMizunoDijkstraScholten;

/*
 * class Receive
 * 
 * As it is commmented on Node class, we will use this class as a thread of Node
 * to be able to receive different messages while the node is working. Here we
 * will define the different kind of messages that can be received by a Node.
 * 
 * If another kind of message is sent we will ignore that one. According with 
 * the message id we will distinguis between messages from the algorithms or 
 * work messages.
 * 
 *  
 * */

import java.io.UnsupportedEncodingException;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import Beanstalk.BeanstalkClient;
import Beanstalk.BeanstalkException;
import Beanstalk.BeanstalkJob;

public class Receive extends Thread{
	/* It is just needed the Node and a BeanstalkClient to receive the messages */
	private  Node node;
	private BeanstalkClient receiveClient;
			
	/* Initialize the constructor with the associated Node */
	public Receive(Node n){	
		node = n;	
		/* Create the BeanstalkClient to receive the messages. It is needed
		 * to distinguish between the client used to send and to receive as if 
		 * we use the same channel the information could be destroyed and we should
		 * implement detection errors protocols to detect it */
		createBeanstalkClient();
		
	}
	
	/* Override the run method to implement the listening of the channel */
	@Override
	public void run(){
		BeanstalkJob job = null;
		
		/* We will listen to till the program is over */
		while(!MainNode.programEnd){
			try {
				job = receiveClient.reserve(5000);
				if (job != null){
					
					identifyMessage(job);
					job = null;
				}
				
			} catch (BeanstalkException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println(e.getMessage());
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
		}
		
	}
	
	
	/* Method to create the client and to tell which tubes will be listening the Node*/
	private void createBeanstalkClient(){
		receiveClient = new BeanstalkClient(MainNode.ip, MainNode.port, "t_"+node.getIdNode());
	}
	
	/*
	 * Method in charge to define where come from the message and and what kind of job
	 * should be done(It will distinguish between a signal or work message)
	 * */
	private synchronized void identifyMessage(BeanstalkJob job) throws UnsupportedEncodingException, BeanstalkException, ParseException{
		Message mensaje = new Message();
		JSONObject jsonObject;
		
		String str = new String(job.getData(),"UTF-8");
		
		/* Delete the job once received */
		receiveClient.deleteJob(job);
		jsonObject = mensaje.stringToJSON(str);
		/* Identify the work to be done */
		identifyWork(jsonObject);
		
	}
	
	/*
	 * Method in charge of executing the assigned job
	 * */
	public synchronized void identifyWork(JSONObject jsonObject){
		
		int id;
		
		Long aux = new Long((long)jsonObject.get("id"));
		id = aux.intValue(); 
		
		/*According to the message received we will execute a couple of instructions */
		switch (id) {
			
			case (Message.ID_INCREMENT):
				
				int count, fromNode;					
				int [] jsonValuesInc = new int[2];
				
				jsonValuesInc = jsonValuesToIncrement(jsonObject);
				fromNode = jsonValuesInc[0];
				count = jsonValuesInc[1];
				
				node.incrementJobs();
				
				/* Receive message and to add into the queue */ 
				try {
					node.receiveMessage(fromNode, count);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				break;
			
			case Message.ID_BLUR:
				int yInit, yEnd;	
				int nodeBlurWork;			
				int [] jsonValuesBlur = new int[2];
				
				jsonValuesBlur = jsonValuesToBlur(jsonObject);
				
				
				nodeBlurWork = jsonValuesBlur[0];
				yInit = jsonValuesBlur[1];
				yEnd = jsonValuesBlur[2];
				
				node.incrementJobs();
				
				/* Receive message and to add into the queue */ 
				try {
					node.receiveMessage(nodeBlurWork, yInit, yEnd);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				break;
			
			
			case Message.ID_REQUEST:
				int [] jsonRequestValues = new int[2];
				int originator, source;
						
				jsonRequestValues = valoresJSONRequest(jsonObject);
				source = jsonRequestValues[0];
				originator = jsonRequestValues[1];
						
				node.receiveRequest(originator, source);
			
				break;
			
			case Message.ID_TOKEN:
				
				aux = (long)jsonObject.get("token");
				int token = aux.intValue();
				
				/* Check if token has been received */
				if (token == 1) node.notifyMethod();
	
				break;
			
			case Message.ID_SIGNAL:
				
				/* Execute the receive signal for the Dijkstra-Scholten algorithm */
				node.receiveSignal();
				/* Execute send Signal once received as it could be last work to be done,
				 * it will depend on the queue and the inDeficit and outDeficit indexes*/
				node.sendSignal();
				
				/*
				 * Note: It is important to apreciate that it is used the same instance Node
				 * so that the use of the same variables is done. We cannot use another method
				 * created in this class to access to the variables. It wouldn't make the changes
				 * because they are done by another Thread on other class. 
				 * 
				 * What it is done is to copy the instance reference and it call its methods. If a 
				 * new Node will have been made it will be another instance on this class and it 
				 * would work
				 * */
				
				break;
			default:
				System.out.println("No he hecho naaaa, id es:"+id+" y JSON"+jsonObject.toString());
		}
	
		
		
	}
	
	
	/*
	 * Method used to get the json values to increment
	 * */
	private int[] jsonValuesToIncrement(JSONObject jsonObject){
		
		int [] result = new int[2];
		Long aux;
		
		aux = (long)jsonObject.get("nodo");
		result[0] = aux.intValue();
		
		aux = (long)jsonObject.get("argumentos");
		result[1] = aux.intValue();
		
		return result;
	}
	
	/*
	 * Method to read the values that the node need to blur
	 * 
	 * */
	private int[] jsonValuesToBlur(JSONObject jsonObject){
		
		int [] result = new int[3];
		Long aux;
		
		aux = (long)jsonObject.get("nodo");
		result[0] = aux.intValue();
		
		aux = (long)jsonObject.get("inicio");
		result[1] = aux.intValue();
		
		aux = (long)jsonObject.get("fin");
		result[2] = aux.intValue();
		
		return result;
	}
	/*
	 * Method to get he json values when a request is used
	 * */
	private int[] valoresJSONRequest(JSONObject jsonObject){
		Long aux;
		int [] result = new int[2];
		
		aux = (long)jsonObject.get("source");
		result[0] = aux.intValue();
		
		aux = (long)jsonObject.get("originator");
		result[1] = aux.intValue();
		
		
		return result;
	}
	
	
	
	
}

