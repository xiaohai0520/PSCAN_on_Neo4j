import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RUN_PSCAN {
	
	static String file = "/Users/xiaohai/Documents/study/class/2018fall/729/assignment5/src/email.txt";
	static String model = "pscan_data";
	
	
	
	
	public static long run_pscan() throws IOException{
		float eps = 0.45f;
		int miu = 2;
		
		System.out.println("eps: " + eps + " " + "miu: " + miu + "\n");
		pscan test = new pscan(file,model,eps,miu);
		
		long startTime=System.currentTimeMillis();
		List<Set<Integer>> clusters = test.pSCAN();
		
		FileOutputStream fileOut =
		new FileOutputStream("clusters.ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(clusters);
		out.close();
		fileOut.close();
		
		
//		System.out.printl(test.core)
		System.out.println("All clusters finally:");
		int i = 1;
		for(Set<Integer> cluster: clusters) {
			
			System.out.println("No." + i + ": " + cluster);
			i++;
		
		}
		long timeUse = System.currentTimeMillis()-startTime;
		System.out.println("Time Use: "+ (timeUse) + "ms");
		return timeUse;
	}
	
	public static void readcluster() {
		List<Set<Integer>> clusters = null;
	      try {
	         FileInputStream fileIn = new FileInputStream("clusters.ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         clusters = (List<Set<Integer>>) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	         return;
	      } catch (ClassNotFoundException c) {
	         System.out.println("Employee class not found");
	         c.printStackTrace();
	         return;
	      }
	      System.out.println(clusters.size());
	}
	
	
	
//	 public static void test_time() throws IOException {
//		float[] cur = new float[4];
//		cur[0] = 0.4f;
//		cur[1] = 0.5f;
//		cur[2] = 0.6f;
//		cur[3] = 0.7f;
//		ArrayList<Long> times = new ArrayList<>();
//		for(int i = 0; i < 4; i++) {
//			times.add(run_pscan(cur[i]));
//		}
//		System.out.println(times);
//		
//	}
	
	
	
	public static void main(String[] args) throws IOException {
		

		run_pscan();
//		readcluster();
	}
	
}




//[4590, 4967, 4841, 4603]    anything
//[4773, 5047, 4991, 4797]   without prune 
// [5087, 5725, 5383, 5309]  without cross- and prune
//[5341, 5777, 5746, 5761] without three
