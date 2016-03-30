
public class Main {
	public static int nodesNum = 0;
	public static int edgesNum = 0;
	public static void main(String[] args) {
//		java.net.URL location = Main.class.getProtectionDomain().getCodeSource().getLocation();
//		System.out.println(location);
		SQLReader t = new SQLReader();
		t.connect();
		try {
			Neo4j myGraph = new Neo4j(t);
			myGraph.startImport();
			myGraph.shutDownDB();
			System.out.println("**************************************");
			System.out.println("******Operation successed!******");
			System.out.println("Finally imported " + nodesNum + " nodes.");
			System.out.println("Finally imported " + edgesNum + " edges.");
		} catch (Exception e) {
			System.out.println("**************************************");
			System.out.println("Operation faild...");
			System.out.println("**************************************");
		} finally {
			t.closeConnect();
		}
	}

}
