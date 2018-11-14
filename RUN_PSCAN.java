import java.io.IOException;

public class RUN_PSCAN {
	
	
	
	public static void main(String[] args) throws IOException {
		
		String file = "/Users/xiaohai/Documents/study/class/2018fall/729/assignment5/src/data1.txt";
		float eps = (float) (2.0f/Math.sqrt(15));
		int miu = 4;
		pscan test = new pscan(file,eps,miu);
		saveNode[] clusters = test.pSCAN();

//		System.out.println(test.clusters);
		for(int i = 0; i < clusters.length;i++) {
			saveNode cluster = clusters[i];
			while(cluster != null) {
				System.out.print(cluster.val + " ");
				cluster = cluster.next;
			}
		System.out.println(" ");
		}
	}
	
}
