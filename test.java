import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class test {
	
	static String answer_file = "/Users/xiaohai/Documents/study/class/2018fall/729/assignment5/src/email-cluster.txt";
	
	static Map<Integer,Set<Integer>> clusters = new TreeMap<>();
	
	public List<String> readanswer(String fileName) throws IOException {
		String fileAllName = fileName;
		File file = new File(fileAllName); 
		InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
		BufferedReader br = new BufferedReader(reader); 
		List<String> list = new ArrayList<String>();
		String line = "";

//		read all nodes into the array
		while((line = br.readLine()) != null) {
//			System.out.println(line);
			if(line.substring(0, 1) == "#") {
				continue;
			}
			
			list.add(line);  
		}
		return list;
	}
	
	public void handleList(List<String> ls) {
//		Map<Integer,ArrayList<Integer>> clusters = new TreeMap<>();
		for(String infos:ls) {
			String[] rela=infos.split(" ");
			int ID = Integer.parseInt(rela[0]);
			int cluster_id = Integer.parseInt(rela[1]);
			
			if(clusters.containsKey(cluster_id)) {
				Set<Integer> cur = clusters.get(cluster_id);
				cur.add(ID);
				clusters.put(cluster_id, cur);
				
			}else {
				Set<Integer> cur = new TreeSet<>();
				cur.add(ID);
				clusters.put(cluster_id, cur);
			}
			
		}
		
//		for(int key:clusters.keySet()) {
//			System.out.println(clusters.get(key));
//		}
		
	}
	
	
	public void test_id(int id,List<Set<Integer>> predict) {
//		int id = 2;
		Set<Integer> guess = null;
		Set<Integer> ans = null;
		//get the guess
		for(Set cur:predict) {
			if(cur.contains(id)) {
				guess = cur;
				break;
			}
		}
		//get the ans
		for(Set cur: clusters.values()) {
			if (cur.contains(id)) {
				ans = cur;
				break;
				
			}
		}
		
		System.out.println("predict: " + predict);
		System.out.println("fact:    " + ans);
		int pre_length = ans.size();
		ans.retainAll(guess);
		float correct_rate = (float)ans.size()/pre_length;
		System.out.println("correct rate: " + correct_rate);
		
	}
	
	public static void main(String[] args) throws IOException {
		
		test a = new test();
		//get answer
		a.handleList(a.readanswer(answer_file));
		
		List<Set<Integer>> predict = new RUN_PSCAN().run_pscan();
		a.test_id(5,predict);
		//compare two part
		
		
	}
}
