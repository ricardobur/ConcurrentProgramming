package NeilsenMizunoDijkstraScholten;

/*
 * class MainNode
 * 
 * We have an unidirectional graph who communicates the nodes. We will suppose we have
 * a main node, node 0, who distribute the work to the rest of the nodes. When a node
 * receive a Message, it would be able to divide its work and send it to their children
 * and so on. Last nodes are called leaves and they can't distribute work to the rest of
 * the nodes.
 * 
 * This class will represent the main node who will create the graph connections and the 
 * nodes according to a file in .dot format. On this file we have the relations between the
 * nodes and the way that they can send messages to each other.
 * 
 * We are going to implement two distributed algorithms:
 * 
 * 1) Algorithm of Neilsen-Mizuno, based on distributed mutual exclusion.
 * 2) Algorithm of Dijkstra-Scholten, based on distributed termination(endless).
 * 
 * These both algorithms are explained on README.txt where we can find some images
 * as they work and they pseudocode.
 * 
 * @author Ricardo Burillo
 * 
 * */



import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.imageio.ImageIO;

import Beanstalk.BeanstalkClient;
import Beanstalk.BeanstalkException;
import Beanstalk.BeanstalkJob;


public class MainNode {
	
	/* Variables to connect the different ports */
	public static int port;
	public static String ip;
	public static int numNodes;
	public static int counter;
	public static int envNodeId;
	public static Node envNode;
	
	/* Path to the graph and .txt files */
	public static final String settingsFile = "nmds.cfg";
	public static String connectionGraph;
	public static String spanningTree;
	public static String counterFile;
	public static String originalImage;
	public static String imageToBlur;
	
	
	/* List of the nodes that send and receive from */
	public static List<String> listSendTo;
    public static List<String> listReceiveFrom;
    
    /* Matrix and images size to work with it */
    public static volatile int[][] imageMatrix;
    public static volatile int height, width;
    
    /* Variable to end the program */
    public static volatile boolean programEnd = false;
    
    /* List to save the nodes and then to kill them */
    private static List<Node> nodeList = new ArrayList<>();
    
    /* Indicates which kind of job the program will execute */
    public static volatile boolean incrementOrBlur;  
	
    
    /* Main program in charge of executing one iteration of for increment or blur an image*/
	public static void main(String[] args) throws Exception {
		/* Variables used to check the time wasted on calculate the time needed */
        double start, end;
		
        /* Initialize variables gotten from settings file */
		initVars();
		
        /* First init Files and the envNode with the graph list */
		initFiles();
	
		/* Create the list of different nodes in the graph */
		for (int i=1; i < numNodes; i++){
			System.out.println("Creo nodo "+i);
			nodeList.add(createNode(i));
		}
		
		if (incrementOrBlur)System.out.println("\n Starting count ...\n");
		else System.out.println("\n Starting image's blured ...");
		
		/* Get the time when is initialized the program execution */
		start=System.nanoTime();
		
		/* Start the send the works and wait till all nodes end*/
		envNode.start();
		System.out.println("Valor de altura y anchura: " + height + " " + width);
		while(!programEnd);
		
		/* Write the Spanning Tree result */
		try {
			envNode.writeSpanningTreeDot();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Get the time when the program to obtain the complete time wasted*/
		end=System.nanoTime();
		
		/* Print the results of the execution */
		System.out.println("\n ********** TIME USED ************* \n");
		System.out.println("           "+((end-start)/1000000000)+" seconds \n");
		System.out.println("Spanning tree generated on spanningtree.png file \n");
		envNode.spanningTreePhoto("spanningtree");
		
		/* If we have chosen increment work we will show its content */
		if (incrementOrBlur)envNode.showFile();
		System.exit(0);
		
	}
	
	
	/*
	 * This function will initialize all the variables with the properties file values.
	 * 
	 * */
	public static void initVars(){
		
		Properties settings = new Properties();
		try {
			FileInputStream props = new FileInputStream(settingsFile);
			settings.load(props);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//TODO: pONER UN DEFAULT VALUE CUANDO SE INICIALIZAN SI NO HAY FICHERO
		
		port = Integer.parseInt(settings.getProperty("port"));
		numNodes = Integer.parseInt(settings.getProperty("num_nodes"));
		counter = Integer.parseInt(settings.getProperty("counter"));
		envNodeId = Integer.parseInt(settings.getProperty("env_node"));
		ip = settings.getProperty("ip");
		
		/* Files paths */
		connectionGraph = settings.getProperty("path_graph_connection"); 
		spanningTree = settings.getProperty("path_spanning_tree");
		counterFile = settings.getProperty("path_counter_file");
		originalImage = settings.getProperty("path_original_image");
		imageToBlur = settings.getProperty("path_blur_image");
		
		/* Content List where we have all nodes to be send or received */
		listSendTo = new ArrayList<>();
	    listReceiveFrom = new ArrayList<>();
	    
	    /* programEnd to indicate the final of the program */
	    programEnd = false;
	    
	    /* Node list to know the nodes we have */
	    nodeList = new ArrayList<>();
	    
	    /* Variable to indicate if we increment or we blur:
	     * 	Blur -> false
	     *  Increment -> true
	     * */
	    incrementOrBlur = Boolean.valueOf(settings.getProperty("inc_or_blur"));   
	    													  	
	}
	
	/*
	 * Method used to initialize all the different files needed according to the execution
	 * */
	public static void initFiles(){
		
		/* First of all we read the graphConnection between the tree nodes */
		try {
			readGraphFileConnections();
			
			/* Initialize the envNode with its Blocking Queue. Here we will already
			 * have the graph connection values on its variables */
			BlockingQueue<Object> q = new ArrayBlockingQueue<Object>(50);
		    try {
				envNode = new Node(envNodeId, q);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Then we init the Image or the increment file according to the 
		 * kind of operation to be done on the interaction */
		if (incrementOrBlur) initIncrementFile();
		else initOriginalImage();
	}
	
	/*
	 * Method used to create a new node and start it like a TODO: Process
	 * 
	 * When we create a node we assign it a BlockingQueue. The reason is that we will
	 * have another thread received where we receive the different messages sent from 
	 * the other nodes and we will use it to distribute the different jobs receive on
	 * the receive thread to be done to the main thread.
	 * 
	 * */
	private static Node createNode(int id){
		
		BlockingQueue<Object> q =  new ArrayBlockingQueue<Object>(50); // Creamos una cola que acepta un máximo de 50 trabajo
		Node nodo = null;
		
		/* Creation of new Node */
		try {
			nodo = new Node(id, q);
			nodo.start();
		} catch (IOException e) {
			// TODO Identificar las correcta excepcion
			System.out.println("Error creating and running Node Object "+ e.getMessage());
			e.printStackTrace();
		}
		return nodo;
	}
	
	/*
	 * This method is in charge of reading the file and distribute the content
	 * in two lists. One, listSendTo tells us who sends and the other list tells
	 * us who receive. According to the index on both lists we know for an index
	 * who sends to whom.
	 * 
	 * */
	private static void readGraphFileConnections() throws IOException{
		
		FileReader fr = new FileReader(new File(connectionGraph));
	    BufferedReader br = new BufferedReader(fr);
	    String str;
	    
	    /* Read all the values in the file to know who sends whom */
	    while((str=br.readLine())!=null && str.length()!=0){
	    	String[] valores = str.split("->");

	    	/* We add the values and remove possible blankspaces introduced */
	    	listSendTo.add(valores[0].replaceAll("\\s", ""));
	        listReceiveFrom.add(valores[1].replaceAll("\\s", ""));

	    }
	    
	    br.close();   
	    
	}
	
	
	/*
	 * Method in charge of initialize the value from the file to comprove that the value has incremented.
	 * We ensure that the file initialize its value with 0.
	 * */
	private static void initIncrementFile(){
		
		DataOutputStream out;
		try {
			out = new DataOutputStream(new FileOutputStream(counterFile));
			out.writeInt(0);
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		envNode.deleteSpanningTreeFile();
		
		
	}
	/*
	 * This method is used to clean the channels used for the nodes to be sure that they don't read
	 * a message they don't have to.
	 * */
	private static void cleanChannels(){
		
		BeanstalkClient cleaner = new BeanstalkClient(ip, port);
		
		System.out.println("\n Limpiando Canales... \n");
		for (int i = 0; i < numNodes; i++){
			
			try {
				cleaner.watchTube("t_"+i);
				BeanstalkJob job = cleaner.reserve(1);
				System.out.println("De "+ i + "Limpio "+job);
				if (job != null) {
					cleaner.deleteJob(job);
					try {
						String str = new String(job.getData(),"UTF-8");
						System.out.println(str);
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
			} catch (BeanstalkException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
	}
	
	/*
	 * Method used to copy the image value inside the changeMatrix. It is used because it will be easier
	 * to save the value in an matrix and operate through that than open and close the file. Even it will
	 * be faster but the memory used. This method will copy the original image into the blurred to make the
	 * process as if it would have never been used.
	 * */
	private static void initOriginalImage(){
		BufferedImage img = null;
		BufferedImage outImage = null;
		
		/* Read the image */
		try {
			outImage = ImageIO.read(new File(imageToBlur));
			img = ImageIO.read(new File(originalImage));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Get the height and width on pixels */
		height = img.getHeight();
		width =  img.getWidth();
		
		/* Create the matrix who will contain the RGB bytes as integer value */
		imageMatrix = new int[height][width];
		
		int pixel;
		
		/* 
		 * Make a copy of the enter image on the matrix and on the output image
		 * where we will make the blur
		 *  */
		int rgb = 0;
		for (int h = 1; h<height; h++){
            for (int w = 1; w<width; w++){
                pixel = img.getRGB(w, h);
                imageMatrix[h][w] = pixel;  
                outImage.setRGB(w, h, rgb);
            }
        }
		/* Write the output image with bmp format */
		try {
			ImageIO.write(outImage, "bmp", new File(imageToBlur));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}


/*
 * Posibles Mejoras
 * 
 * TODO Hacer que sólo leamos del fichero en lugar de guardarlo en una variable global
 * TODO Repasar el método de split de los nodos para que sólo lo haga con un .dot bien hecho(primera línea no la lea x ejemplo)
 * TODO Revisar cleanChannels para asegurarse si es necesario o no
 * TODO
 * */
