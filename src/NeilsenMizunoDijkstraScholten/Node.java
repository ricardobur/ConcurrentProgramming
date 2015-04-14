package NeilsenMizunoDijkstraScholten;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import javax.imageio.ImageIO;

import Beanstalk.BeanstalkClient;
import Beanstalk.BeanstalkException;


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
 * 
 * 
 * Notes:
 * 
 * It is seen in the code that some of the methods are synchronized, specially the send 
 * methods. It is because of the beanstalk library used has no exclusion implemented, so it
 * needed to indicate that when a node, Object Node, is sending not any other node send at
 * the same time. For that we use the synchronized methods, to block the OBJECT (not instance)
 * methods that we don't want to be executed at same time
 * 
 * @author Ricardo Burillo
 */

public class Node extends Thread{
	
	/* Node identifier */
	private int idNode = -1;
	
	/* Beanstalk Client used to send messages */
	private BeanstalkClient sendClient;
	
	/* Thread for receive messages */
	private volatile Receive msgReceiver;
	
	/* List of tubes used to send messages according to the nodes that we can send.
	 * their name will be formed by t_ + idNode
	 * */
	private List<String> sendTubes = new ArrayList<String>();
	
	/* Variables used for Neilsen-Mizuno Algorithm. They are declared volatile as they
	 * will be accessed by two threads, Receive and Node 
	 * */
	public volatile int parent;
	public volatile int deferred;
	public volatile boolean holding; 
	
	/* Variables used for Dijkstra-Scholten Algorithm. As for Neilsen-Mizuno Algorithm they
	 * are volatile for the same reason.
	 *  */
	public volatile int inDeficit;
	public volatile int outDeficit;
	public volatile int parentDS;
	public volatile int inDeficitArray[];
	public volatile boolean isTerminated;

	/* BlockingQueue used to block a Thread while it is waiting for works to get on the queue */
	public volatile BlockingQueue<Object> queue;
	
	/* Matrix used to make operations on the image in case we have to Blur */
	private int[][] changeMatrix;

	/* Used to count the amount of jobs done by a node  */
	private volatile int jobs;
	
	/* Constructor where we initialize all the different variables */
	public Node (int id, BlockingQueue<Object> q) throws IOException{
		
		/* Assign the id and reset jobs number */
		idNode = id;
		jobs = 0; 
		
		/* Initialize Neilsen-Mizuno variables */
		parent = 0;
		deferred = 0;
		holding= idNode == MainNode.envNodeId ? true:false;  // Sólo si es 0, el root, tendrá el poder al principio
		
		/* Initialize Dijsktra-Scholten variables */
		inDeficit = 0;
		outDeficit = 0;
		parentDS = -1;
		inDeficitArray = new int[MainNode.numNodes];
		isTerminated = false;
		
		/* Assign queue given for this node-receive pair */
		queue = q;
		
		/* Create the client to be used as sender on beanstalk */
		createBeanstalkClient();
		
		/* Assign to whom we can send to. We will receive for the same what, t_ + idNode */
		assignGraph();
		
		/* Create the receive object, thread to receive and discriminate the messages */
		msgReceiver = new Receive(this);
		
	}
	
	
	/* Method run() of Thread overriden to make our execution as Node and mixed with the algorithms */
	@Override
	public void run(){
		
		int incrementValue;
		int [] rows;
		
		/* First of all we start the thread in charge of receiving the messages */
		msgReceiver.start();
				
		/* if it is the envNode it will wait till all works are done */
		if (idNode == MainNode.envNodeId){
			
			if (MainNode.incrementOrBlur){
				incrementValue(MainNode.counter);
			}else{
				blurImage(1, MainNode.height - 1);
			}
			
			/* Wait till envNode outDeficit is 0 */
			while(outDeficit != 0);

			/* Once it has finished we indicate this */
			MainNode.programEnd = true;
			
		/* In case it is a normal node we will wait any instruccion from our 
		 * parents node or we will work on the operation
		 * */	
		}else{
			
			while(!MainNode.programEnd){
				
				/* Get increment value from the queue and operate*/
				if (MainNode.incrementOrBlur){
						incrementValue = consumeQueueToIncrement();
						incrementValue(incrementValue);
						
				/* Get the initial row and the end row and operate */
				}else{
					rows = consumeQueueToBlur();
					blurImage(rows[0], rows[1]);
					
				}
					
			}
			
		}
					
	}
	
	/* Method used to consume the different values for the increment case */
	private int consumeQueueToIncrement(){
		int result = -1;
		try {
			result = (int)queue.take();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	/* Method used to consume the different values for the blur case */
	private int[] consumeQueueToBlur(){
		int rows[] = {-1, -1};
		try {
			rows = (int[])queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rows;
	}
	
	/* Create the Beanstalk Client that the will be in charge of sending the messages through the port */
	private void createBeanstalkClient(){
		sendClient = new BeanstalkClient(MainNode.ip, MainNode.port, "t_"+idNode);
	}
	
	
	/* To each one of the nodes we will assign the nodes to whom he can send or receive from */
	private void assignGraph() throws IOException{
		
		/* Lists with the full graph relational contained in the main lists and the id of the node */
	    List<String> sendList= MainNode.listSendTo;
	    List<String> receiveList= MainNode.listReceiveFrom;
	    String idNodeAux = new Integer(idNode).toString();
	    
	    /* Just in case that the node appears on the sendList we will go through
	     * the list to get the different values where the node can send
	     * */
	    if (sendList.contains(idNodeAux)) setReceiveSendIndexs(sendList, receiveList, idNodeAux);
	    
	}
	
	
	/* This function has two functionalities depending on the order of the parameters. In both cases
	 * it gives to the Node the different tubes of Beanstalk where he can receive or not. The parameters
	 * are:
	 * @param sendList: List containing the list of all nodes who sends to other node
	 * @param receiveList: List containing the list of all nodes that receive from other node
	 * @param id: id of the node being treated
	 * @param b: indicates if we are calling the function to fill the sendList(true) or the receiveList(false)
	 * 
	 * The functionalities, as the "b" parameter defines are:
	 * 
	 * 1) 	If we want to fill up the receiveList we will search on the receiveList for node's appear; we will
	 *  	include that value on the sendTubes and we will delete that index from the both lists. It is considered
	 *  	that we can find more that one child to send to, that's the reason we will delete them.
	 *  
	 * 2)   This case is quite similar to the first one with the difference that now we will search on the 
	 * 		sendList instead of the receiveList (as we have changed the lists order's on the parameters field).
	 * 		The other difference is that we will keep the result inside of the tubesReceive as we are detecting
	 * 		which node sends this Node.
	 *  
	 *  */
	private void setReceiveSendIndexs(List<String> sendList, List<String> receiveList, String id){
		
			/* Lists to work with and index of of the Node*/
			List<String> lE = new ArrayList<String>(sendList);
			List<String> lR = new ArrayList<String>(receiveList);
			int index = lE.indexOf(id); 
			
			/* In case we find this node inside of the list we will search all apparitions of this 
			 * node and we will work depending on the call done
			 * */
			while (index != -1) {
				/* Add the value to sendTubes list */
				sendTubes.add("t_"+lR.get(index));
				
				/* Remove and get next index position*/
				lE.remove(index);
				lR.remove(index);
				index = lE.indexOf(id);
				
						
			}
	}
	
	
	/*************************************************************************
	* 																		 *
	* 																		 *  
	*  		Methods Related to Dijsktra-Scholten						     *
	* 																		 *  
	* 																		 *  
	*************************************************************************/
	
	/* Method in charge of sending the signal to the parent once it has finished */
	public synchronized void sendSignal(){
		
		/* Message declaration, info of the message to be sent and the tube */
		Message msg = new Message(Message.ID_SIGNAL, this);
        String info = msg.jsonToStringSignal(idNode);
        String tube;
        
       
        	/* Implementation of the Dijkstra-Scholten receiveSignal Algorithm*/
        	if (inDeficit > 1) {
	        	for (int edge = 0; edge < this.inDeficitArray.length && inDeficit > 1; edge++){
	        		
	        		tube = "t_"+edge;
	    			if (inDeficitArray[edge] > 1 || (inDeficitArray[edge] == 1 && edge != parentDS)) {
	    				/* Send the message through to the node who sent us the work*/
	                    try {
							sendClient.useTube(tube);
							sendClient.put(1l, 0, 5000, info.getBytes());
						} catch (BeanstalkException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	                    
	                   
	                    /* Decrement the nodes we due any work */
	                    inDeficitArray[edge]--;
	                    inDeficit--;
	                   
	                }
	    			
	        	 }
	        
	        /* Check the case when we due to the node's parent. According to Dijkstra-Scholten 
	         * algorithm we check its conditions of terminated, to be the last who we due a work
	         * and any other child of us due a work. As we use a BlockingQueue for the consumer-producer
	         * problem we will have to check that this is empty to be sure that when this node has accomplish
	         * the conditions another work haven't enter to the queue.
	         *  */
	        }else if (inDeficit == 1 && isTerminated && outDeficit == 0 && queue.size() == 0) {
				
	        	/* Send message to its parent */
	            try {
					sendClient.useTube("t_" + parentDS);
					sendClient.put(1l, 0, 5000, info.getBytes());
				} catch (BeanstalkException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            
	            /* Reset the "works duty" to other nodes and its parent*/
	            inDeficitArray[parentDS] = 0;
	            inDeficit = 0;
	            parentDS = -1;
	        }
        	
       
	}
	
	/* Receive Signal method for Dijkstra-Scholten. This will be used by Receive object */
	public synchronized void receiveSignal(){
		outDeficit--;
	}

	/* Method for work in charge of receive the message for the increment option */
	public synchronized void receiveMessage(int nodo, int contador) throws Exception{
		isTerminated = false;

		if (parentDS == -1){
			// Aquí cambiamos la información del parent por lo que guardaremos la información del Spanning Tree
			parentDS = nodo;
			writeSpanningTreeInfo(parentDS, idNode);
		}
		
		inDeficit++;
		inDeficitArray[nodo]++;
		
		try {
			queue.put(contador);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* Method for work in charge of receive the message for the blur option */
	public synchronized void receiveMessage(int nodo, int inicio, int fin) throws Exception{
		isTerminated = false;
		
		if (parentDS == -1){
			// Aquí cambiamos la información del parent por lo que guardaremos la información del Spanning Tree
			parentDS = nodo;
			writeSpanningTreeInfo(parentDS, idNode);
		}
		
		inDeficit++;
		inDeficitArray[nodo]++;
		
		int[] inicioFin = new int[2];
		
		inicioFin[0] = inicio;
		inicioFin[1] = fin;
		try {
			queue.put(inicioFin);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*************************************************************************
	* 																		 *
	* 																		 *  
	*  		           Methods Related to Neilsen-Mizuno				     *
	* 																		 *  
	* 																		 *  
	*************************************************************************/
	
	
	/*
	 * Method to check the input on the critic section of Neilsen-Mizuno
	 * */
	public synchronized void criticSectionEntranceNM(){
		
		if (!holding){
			/* We ask for the token if we don't have it */
			sendTokenRequest();
			parent = 0;
			/* And we wait till we get the notify once we receive the token (notify in receive Thread Node) */
			try {
				this.waitMethod(); 

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		holding = false;
	}
	
	/*
	 * Method executed when we go out from the critic section according to Neilsen-Mizuno
	 * */
	public synchronized void criticSectionExitNM(){
		if (deferred != 0){
			/* Send token to the deferred node that is waiting for it */
			sendTokenToDeferred();
			deferred = 0;
		}else holding = true;
	}
	
	/*
	 * Method in charge of requesting the token to be able to write on the centralized file
	 * */
	private synchronized void sendTokenRequest(){
		Message msg = new Message(Message.ID_REQUEST, this);
		String requestTokenMessage;
		try {
			/* We send the originator of the token request and who is requesting it */
			requestTokenMessage = msg.jsonToStringNM(idNode, idNode);
			
			/* We send the request to the parent */
			sendClient.useTube("t_"+parent);
			sendClient.put(1l, 0, 5000, requestTokenMessage.getBytes());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BeanstalkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Method in charge to send the token to the Node that ask for access to it
	 * */
	private synchronized void sendTokenToDeferred(){ //int deferred
		
		Message msg =  new Message(Message.ID_TOKEN, this);
		String tokenMessage;
		try {
			tokenMessage = msg.jsonToStringToken();
			
			/* Indicate that we will use the deferred tube to send */
			sendClient.useTube("t_"+deferred);
			sendClient.put(1l, 0, 5000, tokenMessage.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BeanstalkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	////////////////////   USED BY Neilsen-Mizuno RECEIVE   ////////////////////////////	
	/*
	 * Method used to send the token to the originator of the request to get the token. This
	 * method is used for the Receive class to send through the method receiveRequest
	 * 
	 * 
	 * @arguments:
	 * 
	 * 		- originator: idNode that started the request to get the token
	 * 
	 * */
	public synchronized void sendTokenToOriginator(int originator){
		
		Message msg =  new Message(Message.ID_TOKEN, this);
		String mensajeToken;
		try {
			mensajeToken = msg.jsonToStringToken();
			
			/* Send message to the originator of the token request*/
			sendClient.useTube("t_"+originator);
			sendClient.put(1l, 0, 5000, mensajeToken.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BeanstalkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * Method in charge to send the request to its parent. It is used to send a message where
	 * it is indicated who sends this message(source = idNode) and who sent the message (originator)
	 * */
	public synchronized void sendRequestToParent(int originator){
		
		Message msg = new Message(Message.ID_REQUEST, this);
		String mensajeRequestParent;
		try {
			mensajeRequestParent = msg.jsonToStringNM(idNode, originator);
			
			/* Send message to its parent */
			sendClient.useTube("t_"+parent);
			sendClient.put(1l, 0, 5000, mensajeRequestParent.getBytes());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BeanstalkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*************************************************************************
	* 																		 *
	* 																		 *  
	*  		           Methods Related to File Operation				     *
	* 																		 *  
	* 																		 *  
	*************************************************************************/
	
	/*
	 * Method in charge of printing what is there in the file in that moment
	 * */
	public synchronized void showFile(){
		DataInputStream in;
		try {
			in = new DataInputStream(new FileInputStream(MainNode.counterFile));
			int resultado = in.readInt();
			System.out.println("^^^^^^^^^ File Value ^^^^^^^^^  "+resultado);
			in.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	/*
	 * Method in charge of writing the value in the centralized file
	 * */
	private synchronized void writeInFile(int contador){
		
		/* We open the file to read and write the new content*/
		try {
			DataInputStream in = new DataInputStream(new FileInputStream(MainNode.counterFile));
			

			int result = in.readInt();
			in.close();
			result = result + contador;
			
			System.out.println("Node "+this.getIdNode()+" adds: "+contador+"---- New File Value: "+result);
			System.out.print("Number end works "+this.jobs);
			
			DataOutputStream out = new DataOutputStream(new FileOutputStream(MainNode.counterFile));
			out.writeInt(result);
			out.close();
						
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

		
	/* Method called to deleted the already created spanningTree for last execution */
	public void deleteSpanningTreeFile(){
		File fileST = new File(MainNode.spanningTree);
        if (fileST.exists()) {
        	fileST.delete();
        }
	}
	/* Method called to create the new photo of the spanning tree */
	public void spanningTreePhoto(String file) throws Exception {
        File fileST = new File(MainNode.spanningTree);
        FileWriter fr = new FileWriter(fileST, true);
        fr.close();
        Runtime.getRuntime().exec(new String[]{"dot", "-Tpng", file + ".dot", "-o", file + ".png"});
    }
	/*
	 * Method used to write the .dot spanning tree file once we have finished
	 * */
	public void writeSpanningTreeDot() throws Exception {
        
        File fileST = new File(MainNode.spanningTree);
        FileWriter fr = new FileWriter(fileST, true);

        fr.write("digraph G {\n");

        /* Create temporary files to build the spanning tree connections */
        for (int i = 1; i < MainNode.numNodes; i++) {
            FileReader infoFile = new FileReader("info" + i + ".txt");
            BufferedReader br = new BufferedReader(infoFile);

            String conexion = br.readLine();
            
            fr.write(conexion+"\n");
            br.close();
            infoFile.close();
            
            /* Once finished we delete the info files */
            File readFile = new File("info" + i + ".txt");
            readFile.delete();
        }
        fr.write("}");
        fr.close();
        
    }

	/*
	 * Method used to write each spanning tree node information every time a Node change its parentDS
	 * */
	public void writeSpanningTreeInfo(int parent, int node) throws Exception {
        File infoFile = new File("info" + node + ".txt");
        FileWriter fr = new FileWriter(infoFile);
        
        fr.write(parent + " -> " + node + "\n");
        fr.close();

    }
	
	
//////////////////// Methods related with the kind of work and divide it /////////////////////////////////	
	
	/*
	 * Method in charge of increasing the value till the desired amount. We will pass
	 * the increment value in case that it has been the desired option and here we 
	 * will divide the work, send it to the other nodes and write into the file
	 * */
	private synchronized void incrementValue(int maxCount){
		
		/* Fisrt we divide the work that has to do the nodes */
		int maxLocalCounter = divideWork(maxCount);
		
		int counter; 
		
		/* First we count */
		for (counter = 0; counter < maxLocalCounter; counter++);
		
		/* Ask for the token according to the Neilsen-Mizuno Algorithm*/
		criticSectionEntranceNM();
		
		/* Token gotten so we write into the file */
		writeInFile(counter);
		
		/* We exit from the critical section */
		criticSectionExitNM();
		
		/* Tell that we have finished the work. To consider that it has
		 * completely finished the work the queue has to be empty too. It could
		 * be that in this place another work has already come inside the queue*/
		isTerminated = true;
		
		/* Send the signal once finished to its parent to indicate that one job was finished*/
		sendSignal();
		
	}
	
	/*
	 * Method in charge of detecting the token has arrived or that we have to send it to the 
	 * originator
	 * */
	public synchronized void receiveRequest(int originator, int source){
		if (parent == 0){
			if (holding){
				/* If we have the token we send it to the originator who asked for it*/
				sendTokenToOriginator(originator);
				holding = false;
			/* Else we will send it once we get it */
			}else deferred = originator;
		}else{
			/* If the node who check it is not the parent, we ask for it to the parent */
			sendRequestToParent(originator);
		}
		
		/* Refresh nodes parent */
		parent = source;
	}
	
	
	/*
	 * Method in charge of dividing the work that has to be done by the others nodes
	 * */
	private synchronized int divideWork(int maxCount){
		
		int valueToCount = maxCount;
		int sendChildrenSet = sendTubes.size();
		/* Check if it has something to count and if there is any children to send to*/
		if ((sendChildrenSet > 0) && (maxCount > 1)){
			
			/* Divide the work between the node and its children and send it  */
			valueToCount = valueToCount / (sendChildrenSet +1); 
			sendWork(valueToCount);
			valueToCount = maxCount-(valueToCount*sendChildrenSet);
			
		}
		
		/* return the value that the node itself has to count */
		return valueToCount;
		
	}
	
	/*
	 * Method in charge of generating the message that we will send to the reception nodes
	 * with the work that they will have to do
	 * */
	private synchronized void sendWork(int counter){
		
		Message message = new Message(Message.ID_INCREMENT, this);
		String tube;
		
		/* Send the work message to all of its children*/
		for (int i = 0; i < sendTubes.size(); i++){
			System.out.println("Envío por tubo "+this.sendTubes.get(i));
			
			
			
			/* It will be send just when it has parent and it is not the root node */ 
			if (parentDS != -1 || idNode == 0) {
				tube = sendTubes.get(i);
				try {
					sendClient.useTube(tube);
					sendClient.put(1l, 0, 5000, message.incrementWork(counter).getBytes());
					/* Increment the number of works sent */
					outDeficit++;
					
				} catch (BeanstalkException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Method used to make that the same Thread wait to the other one till it sends
	 * the signal
	 * */
	public synchronized void waitMethod(){
		
		try {	
			wait();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*
	 * Method called from the Receive.java Thread created for the node. It is important
	 * to realise that to call notify IT IS NEEDED TO USE THE SAME INSTANCE THAT THE 
	 * OBJECT THAT WE WANT TO NOTIFY TO. So, as we want to notify to Node itself, we will
	 * have to call this instance from the receive method (but calling node.notifyMethod
	 * */
	public synchronized void notifyMethod(){
		try {
			notify();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}
	
	
	public int getIdNode(){
		return idNode;
	}
	
	public int getParent(){
		return parent;
	}
	
	public int getDeferred(){
		return deferred;
	}
	
	public boolean getHolding(){
		return holding;
	}
	
	public void incrementJobs(){
		jobs++;
	}
	
	/*
	 * Method used to blur the image. In the queue we will get a Object containing two int values
	 * referred to the beginning and the end of the rows to be blurred. The work to be done will be
	 * divided as well in this function made by rows. These pixels will be blurred according to its position
	 * */
	public synchronized void blurImage(int yInit, int yEnd){
		
		int myWorkRowEnd;
		int totalRows = yEnd - yInit;
		int range;
		
		/* We get the works between we have to operate to */		
		range = divideCoordenatesToBlur(totalRows);
		
		/* The node who divides the work get the first rows and send the oders as children work*/
		myWorkRowEnd = yInit + (totalRows - sendTubes.size()*range);
		sendBluredJob(range, myWorkRowEnd);
		
		/* Calculate the rows where we have to work through */
		calculateNodeRows(yInit, myWorkRowEnd);
		
		/* Same as for incrementValue just that we will write an image instead of the file*/
		criticSectionEntranceNM();
		
		writeImage(yInit, myWorkRowEnd);
		
		criticSectionExitNM();
			
		isTerminated = true;

		sendSignal();	
		
	}
	
	/* Method used to write the image according to a beginning and an end */
	private synchronized void writeImage(int init, int end){
		
		if (end > MainNode.height) end =  MainNode.height - 1;
		BufferedImage imgSalida = null;
		try {
			imgSalida = ImageIO.read(new File(MainNode.imageToBlur));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/* Set RGB values according to the matrix used to change*/
		for (int h = init; h<end; h++){
            for (int w = 1; w < MainNode.width - 1; w++){
            	imgSalida.setRGB(w, h, changeMatrix[h][w]);
            }
        }
		
		try {
			ImageIO.write(imgSalida, "bmp", new File(MainNode.imageToBlur));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* Method to divide the coordenates used to blur the image*/
	public synchronized int divideCoordenatesToBlur(int totalRows){
		return  totalRows / (sendTubes.size() + 1);
	}
	
	/* Similar to sendWork but for the blur image works */
	public synchronized void sendBluredJob(int range, int yInit){
		
		Message msg = new Message(Message.ID_BLUR, this);
		
		int yInitWork, yEndWork;
		
		yInitWork = yInit;
		yEndWork = yInit + range;
		
		if (yEndWork > MainNode.height) yEndWork = MainNode.height;
		String tube;
		
		for (int i = 0; i < sendTubes.size(); i++){
			if (parentDS != -1 || idNode == 0) {
				tube = sendTubes.get(i);
				try {
					sendClient.useTube(tube);
					sendClient.put(1l, 0, 5000, msg.blurWork(yInitWork, yEndWork).getBytes());
				
					yInitWork = yEndWork;
					yEndWork = yInitWork + range;
					
					outDeficit++;	
				} catch (BeanstalkException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/* Method to calculate the rows that has to use the node */
	public void calculateNodeRows(int init, int end){

		if (end > MainNode.height - 1) end = MainNode.height - 1;
		
		changeMatrix =  new int[end][MainNode.width];
		
		/* For the given rows, we blur the pixel*/
		for (int h = init; h<end; h++){
            for (int w = 1; w < MainNode.width - 1; w++){
                changeMatrix[h][w] = blurPixel(h,w);
            }
        }
		
	}
	
	/*
	 * Method used to blur the image. The blur is done according to the 8 closest pixels and 
	 * in case that we find a border we will give it the same value as the border to make the 
	 * blur.
	 * */
	private int blurPixel(int i, int j){
		
		/* Use 5 pixels to make the blur of the image */
		int []rgb_ij = new int[3];   //i,j
		int []rgb_i1j = new int[3];  //i+1,j
		int []rgb_ij1 = new int[3];  //i,j+1
		int []rgb_i_1j = new int[3]; //i-1,j
		int []rgb_ij_1 = new int[3]; //i,j-1
		
		int []pixelDifuminado = new int[3];
		
		/* Get the RGB values */
		rgb_ij = getRGB(MainNode.imageMatrix[i][j]);
		rgb_i1j = getRGB(MainNode.imageMatrix[i+1][j]);
		rgb_ij1 = getRGB(MainNode.imageMatrix[i][j+1]);
		rgb_i_1j = getRGB(MainNode.imageMatrix[i-1][j]);
		rgb_ij_1 = getRGB(MainNode.imageMatrix[i][j-1]);
		
		/* Apply the formula for three colors to get blurred */
		pixelDifuminado[0] = (0*rgb_ij[0]+ 0*rgb_i1j[0]+4*rgb_ij1[0]+ 4*rgb_i_1j[0]+0*rgb_ij_1[0])/16;
		pixelDifuminado[1] = (0*rgb_ij[1]+ 2*rgb_i1j[1]+0*rgb_ij1[1]+4*rgb_i_1j[1]+0*rgb_ij_1[1])/16;
		pixelDifuminado[2] = (0*rgb_ij[2]+ rgb_i1j[2]+4*rgb_ij1[2]+rgb_i_1j[2]+0*rgb_ij_1[2])/12;
		
		/* Convert the RGB value to the image desired value, COlor object */
		Color colorDif = new Color(pixelDifuminado[0], pixelDifuminado[1], pixelDifuminado[2]);
		
		return colorDif.getRGB();
	}
	
	/*
	 * Method in charge to separate the red, green and blue values
	 * */
	private int[] getRGB(int rgb){
		int [] result = new int[3];
		
		result[0] = (rgb >> 16 ) & 0x000000FF;
		result[1] = (rgb >> 8 ) & 0x000000FF;
		result[2] = (rgb) & 0x000000FF;
		
		return result;
	}
}
