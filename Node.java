import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Node implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int id;
	int port;
	int bsPort;
	String type;
	public int getBsPort() {
		return bsPort;
	}
	public void setBsPort(int bsPort) {
		this.bsPort = bsPort;
	}
	String ipAddress;
	Node prevNode;
	Node nextNode;
	Map<Integer,String> consistentMap=new HashMap<>();
	int startPoint;
	int endPoint;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	@Override
	public String toString() {
		return "Node [id=" + id + ", port=" + port + ", ipAddress=" + ipAddress + ", prevNode=" + prevNode
				+ ", currNode=" + nextNode + "]";
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public Node getPrevNode() {
		return prevNode;
	}
	public void setPrevNode(Node prevNode) {
		this.prevNode = prevNode;
	}
	public Node getNextNode() {
		return nextNode;
	}
	public void setNextNode(Node nextNode) {
		this.nextNode = nextNode;
	}
	
}
