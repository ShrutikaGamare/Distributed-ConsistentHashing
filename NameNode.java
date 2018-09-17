import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class NameNode {

	
	public static void main(String[] args) throws NumberFormatException, IOException {
		
		NameNode nameNode=new NameNode();
		nameNode.readFile(args[0]);
	}

	private void readFile(String fileName) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		String fname=fileName;
		fname="nsConfig.txt";
		File file=new File(fname);
		BufferedReader reader=new BufferedReader(new FileReader(file));
		String line=null;
		int count=0;
		Node node=new Node();
		while((line=reader.readLine()) !=null)
		{
			if(count==0)
			{
				node.id=Integer.parseInt(line);
			}
			else if(count==1)
			{
				node.port=Integer.parseInt(line);
				node.ipAddress="localhost";
			}
			else
			{
				String[] data=line.split(" ");
				node.ipAddress=data[0];
				node.bsPort=Integer.parseInt(data[1]);
			}
			count++;
		}
		node.endPoint=node.id;
		startServer(node);
	}

	private void startServer(Node node) {
		// TODO Auto-generated method stub
		new Thread(new NameServer(node)).start();
		new Thread(new Handler(node)).start();
	}
}
class NameServer implements Runnable{

	    Node node=null; 
	    ServerSocket serverSocket=null;
	    Socket socket=null;
	    ObjectOutputStream outputStream=null;
	    ObjectInputStream inputStream=null;
	public NameServer(Node node) {
		// TODO Auto-generated constructor stub
		this.node=node;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			serverSocket=new ServerSocket(node.port);

			while(true)
			{
				socket=serverSocket.accept();
				new Thread(new ReqHandler(socket)).start();
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		
	}

	
	
	class ReqHandler implements Runnable
	{
		 ServerSocket serverSocket=null;
		    Socket socket=null;
		    ObjectOutputStream outputStream=null;
		    ObjectInputStream inputStream=null;

		public ReqHandler(Socket socket) {
				// TODO Auto-generated constructor stub
			this.socket=socket;
			}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			try {
				while(true)
				{
					
				
				outputStream=new ObjectOutputStream(socket.getOutputStream());
				inputStream=new ObjectInputStream(socket.getInputStream());



				String request=(String)inputStream.readObject();
				if(request.startsWith("lookup"))
				{   
					String[] data=request.split(" ");
					lookupKey(data[1]);
					break;
				}
				else if(request.startsWith("insert"))
				{
					String[] data=request.split(" ");
					insertKey(data[1],data[2]);
					break;
				}
				else if(request.startsWith("delete"))
				{
					String[] data=request.split(" ");
					deleteKey(data[1]);
					break;
				}
			   else if(request.startsWith("enter"))
				   {  
				   registerNode();
				   break;
			   }
			   else if(request.startsWith("exit"))
			   {
				   deregisterNode();
				   break;
			   }
			   else if(request.startsWith("update"))
			   {
				   updateNext();
				   break;
			   }
			}
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
		}
		
		private void updateNext() {
			// TODO Auto-generated method stub
			try {
				Node next=(Node)inputStream.readObject();
				node.nextNode=next;
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		private void deregisterNode() {
			// TODO Auto-generated method stub
			try {
				String reqType=(String) inputStream.readObject();
				if(reqType.startsWith("set next"))
				{
					Node nextNode=(Node) inputStream.readObject();
					System.out.println("After exit of next node "+node.nextNode.id);

					node.nextNode=nextNode;
					System.out.println("New next node for "+node.id+" is "+node.nextNode.id);
				}
				else
				{
				   System.out.println("After exit of prev node "+node.prevNode.id);

					Node currPrev=node.prevNode;
					Node newPrev=(Node)inputStream.readObject();
					Map<Integer,String> updatedRange=(Map<Integer, String>) inputStream.readObject();
					node.consistentMap.putAll(updatedRange);
					node.prevNode=newPrev;
					node.startPoint=currPrev.startPoint;
                    System.out.println("Curr prev id is "+currPrev.id +" start point "+currPrev.startPoint);
					System.out.println("New prev node for "+node.id+" is "+node.prevNode.id);
					System.out.println("New range for "+node.id+ " is "+node.startPoint+" to "+node.endPoint);
				}
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}

		private void registerNode() {
			// TODO Auto-generated method stub
			try {
				Node entryNode=(Node) inputStream.readObject();
				int id=entryNode.id;
				if(node.startPoint<=id && node.endPoint>=id)
				{Map<Integer,String> retainedMap=new HashMap<>();
				Map<Integer,String> splitMap=new HashMap<>();

				retainedMap.put(0,node.consistentMap.get(0));
				for(int i=id+1;i<node.endPoint;i++)
				{
					retainedMap.put(i, node.consistentMap.get(i));
				}
				for(int i=node.startPoint;i<=id;i++)
				{    
					if(i!=0) {
						splitMap.put(i, node.consistentMap.get(i));
					}
				}
				node.consistentMap=new HashMap<>(retainedMap);
				outputStream.writeObject("proceed");
				Node prev=node.prevNode;
				outputStream.writeObject(prev);
				outputStream.writeObject(node);
				node.prevNode=entryNode;
				//node.nextNode=entryNode;
				//outputStream.writeObject(next);
				outputStream.writeObject(splitMap);
				node.startPoint=id+1;
				//System.out.println("New start point of Accepted Server is "+node.startPoint+" end point is "+node.endPoint);

				}
				else
				{
					System.out.println("Contact "+node.nextNode.id);

					outputStream.writeObject("contact next");
					outputStream.writeObject(node.nextNode);
					outputStream.writeObject(String.valueOf(node.id));
				}
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			;
		}

		private void deleteKey(String key) {
			// TODO Auto-generated method stub
			int deleteKey=Integer.parseInt(key);
			
			
				if(deleteKey>=node.startPoint && deleteKey<=node.endPoint)
				{
					if(node.consistentMap.get(deleteKey)!=null)
					{
						try {
							node.consistentMap.remove(deleteKey);
							outputStream.writeObject("Successful Deletion");
							outputStream.writeObject(String.valueOf(node.id));

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else
					{
						try {
							outputStream.writeObject("Key not Found");
							outputStream.writeObject(String.valueOf(node.id));

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				else
				{
					 try {
							Socket contactSocket=new Socket(node.nextNode.ipAddress, node.nextNode.port);
							ObjectOutputStream ostream=new ObjectOutputStream(contactSocket.getOutputStream());
							ObjectInputStream iStream=new ObjectInputStream(contactSocket.getInputStream());
							String path="";
							ostream.writeObject("delete "+key);
							String nextRes=(String) iStream.readObject();
							path=(String) iStream.readObject();
							path=path+"-"+String.valueOf(node.id);
							outputStream.writeObject(nextRes);
							outputStream.writeObject(path);
							contactSocket.close();
							ostream.close();
							iStream.close();
						} catch (IOException | ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
			
			
		}

		private void lookupKey(String key) {
			// TODO Auto-generated method stub
			System.out.println("look up called");
			int search=Integer.parseInt(key);
			
				if(search>=node.startPoint && search<=node.endPoint)
				{
					if(node.consistentMap.get(search)!=null)
					{
						try {
							outputStream.writeObject(node.consistentMap.get(search));
							outputStream.writeObject(String.valueOf(node.id));
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else
					{
						try {
							outputStream.writeObject("Key not Found");
							outputStream.writeObject(String.valueOf(node.id));

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				 else
			     {
			    	 try {
						Socket contactSocket=new Socket(node.nextNode.ipAddress, node.nextNode.port);
						ObjectOutputStream ostream=new ObjectOutputStream(contactSocket.getOutputStream());
						ObjectInputStream iStream=new ObjectInputStream(contactSocket.getInputStream());
						String path="";
						ostream.writeObject("lookup "+key);
						String nextRes=(String) iStream.readObject();
						path=(String) iStream.readObject();
						path=path+"-"+String.valueOf(node.id);
						outputStream.writeObject(nextRes);
						outputStream.writeObject(path);
						contactSocket.close();
						ostream.close();
						iStream.close();
					} catch (IOException | ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			     }
			
			
		}

		private void insertKey(String key, String value) {
			// TODO Auto-generated method stub
			int search=Integer.parseInt(key);
			
				System.out.println("Next node is null");
				if(search>=node.startPoint && search<=node.endPoint)
				{
					
						try {
							node.consistentMap.put(Integer.parseInt(key), value);
							outputStream.writeObject("Successfully inserted key at "+node.id);
							outputStream.writeObject(String.valueOf(node.id));

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					
					
				}
				else
				{
				
					try {
						Socket contactSocket=new Socket(node.nextNode.ipAddress, node.nextNode.port);
						ObjectOutputStream ostream=new ObjectOutputStream(contactSocket.getOutputStream());
						ObjectInputStream iStream=new ObjectInputStream(contactSocket.getInputStream());
						String path="";
						ostream.writeObject("insert "+key +" "+value);
						String nextRes=(String) iStream.readObject();
						path=(String) iStream.readObject();
						path=path+"-"+String.valueOf(node.id);
						outputStream.writeObject(nextRes);
						outputStream.writeObject(path);
						contactSocket.close();
						ostream.close();
						iStream.close();
					} catch (IOException | ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			
		}
		
	}
}

class Handler implements Runnable{
	Socket socket=null;
	ObjectOutputStream outputStream;
	ObjectInputStream inputStream;
	
	Node node=null;

	public Handler(Node node) {
		// TODO Auto-generated constructor stub
		this.node=node;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Scanner sc=new Scanner(System.in);
		while(true)
		{
			System.out.println("Enter command");
			String command=sc.nextLine();
			if(command.startsWith("enter"))
			{
				registerNode(node.bsPort,node.ipAddress ,"");
			}
			else if(command.startsWith("exit"))
			{
				deregisterNode();
			}
		}
		
	}

	private void deregisterNode() {
		// TODO Auto-generated method stub
		try {
			Socket prevSocket=new Socket(node.prevNode.ipAddress,node.prevNode.port);
			outputStream=new ObjectOutputStream(prevSocket.getOutputStream());
			inputStream=new ObjectInputStream(prevSocket.getInputStream());
			outputStream.writeObject("exit");
			outputStream.writeObject("set next");
			outputStream.writeObject(node.nextNode); 
			outputStream.close();
			inputStream.close();
			prevSocket.close();
			Socket nextSocket=new Socket(node.nextNode.ipAddress, node.nextNode.port);
			outputStream=new ObjectOutputStream(nextSocket.getOutputStream());
			inputStream=new ObjectInputStream(nextSocket.getInputStream());
			outputStream.writeObject("exit");
			outputStream.writeObject("set prev");
			outputStream.writeObject(node.prevNode);
			outputStream.writeObject(node.consistentMap);
			outputStream.close();
			inputStream.close();
			nextSocket.close();
			System.out.println("Successful exit");
			System.out.println("Id of successor server is "+node.nextNode.id);
			System.out.println("Key range handed is "+node.startPoint+" to "+node.endPoint);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void registerNode(int port, String ip, String path) {
		// TODO Auto-generated method stub
		 try {
			 Socket tsocket=new Socket(ip,port);
			  outputStream=new ObjectOutputStream(tsocket.getOutputStream());
			  inputStream=new ObjectInputStream(tsocket.getInputStream());


			outputStream.writeObject("enter");
			outputStream.writeObject(node);


			String response=(String) inputStream.readObject();
			if(response.equals("proceed"))
			{
				Node prevNode=(Node) inputStream.readObject();
				node.endPoint=node.id;
				Node nextNode=(Node) inputStream.readObject();
				path=path+"->"+nextNode.id;
                System.out.println("Stats of prev node  id "+prevNode.id+" start "+prevNode.startPoint+" end "+prevNode.endPoint);
				node.startPoint=prevNode.id+1;

				node.nextNode=nextNode;
				node.prevNode=prevNode;
				outputStream.writeObject(node.startPoint);
				Map<Integer,String> consistentMap=new HashMap<>();
				consistentMap=(Map<Integer, String>) inputStream.readObject();
				node.consistentMap=new HashMap<>(consistentMap);
				Socket contactSocket=new Socket(node.prevNode.ipAddress,node.prevNode.port);
				ObjectOutputStream ost=new ObjectOutputStream(contactSocket.getOutputStream());
				ObjectInputStream ist=new ObjectInputStream(contactSocket.getInputStream());
				ost.writeObject("update next");
				ost.writeObject(node);
				ost.close();
				ist.close();
				contactSocket.close();
				System.out.println("Successfully registered");
				System.out.println("List of servers traversed are "+path);
				System.out.println("Range managed by current node is "+node.startPoint+" to "+node.endPoint);
				System.out.println("Id of predecessor server is "+node.prevNode.id);
				System.out.println(" The id of successor is "+node.nextNode.id);
				
			}
			else
			{
				Node node=(Node) inputStream.readObject();
				path=path+"->"+inputStream.readObject();
				
				registerNode(node.port,node.ipAddress,path);
			}
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
