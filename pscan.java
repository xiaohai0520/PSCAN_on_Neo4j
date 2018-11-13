import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Math; 

public class pscan {

	static String file = "/Users/xiaohai/Documents/study/class/2018fall/729/assignment5/src/data.txt";
	static String graphDatabase = "pscan_data";
	GraphDatabaseService model;
	BatchInserter myInsert;
	int nodeNumber;
	
	int[] degree;
	int[] similarDegree;//number of adjacent edges with similarity no less than epsilon
	int[] effectiveDegree;//number of adjacent edges not pruned by similarity
	
	float eps = 0.5f;
	int miu = 2;
	
	int[] pa;
	int[] rank;	//pa and rank use for disjoint-set data structure
	
	int[] cid;// cluster id
	
	int[] min_cn;	//minimum common neighbor: -2 means not similar; -1 means similar; 0 means not sure; > 0 means the minimum common neighbor
	
	boolean[] explored;
	ArrayList<saveNode> dis_joint;
	HashMap<String,Float> similars;
	
	
	public pscan(String file) throws IOException {
		try {
			FileUtils.deleteRecursively( new File(graphDatabase));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		List<String> datas = readData(file);
		nodeNumber = createDatabase(datas,graphDatabase);
		
		
		
		model = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(graphDatabase))
                .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
                .setConfig(GraphDatabaseSettings.string_block_size, "60")
                .setConfig(GraphDatabaseSettings.array_block_size, "300")
                .newGraphDatabase();
		
		registerShutdownHook( model );
		
		
		explored = new boolean[nodeNumber];
		dis_joint = new ArrayList<>();
		for(int i = 0; i < nodeNumber;i++) {
			explored[i] = false;
			dis_joint.add(new saveNode(i));
		}
		
		similars = new HashMap<>();
		
		
		
	}
	
	public static void main(String[] args) throws IOException {
		
		pscan test = new pscan(file);
		test.pSCAN();
//		System.out.println(test.dis_joint);
		for(saveNode node : test.dis_joint) {
			while(node != null) {
				System.out.print(node.val);
				node = node.next;
			}
			System.out.println(" ");
		}
	}
	
	
	private void registerShutdownHook(GraphDatabaseService graphDb) {
		// TODO Auto-generated method stub
		Runtime.getRuntime().addShutdownHook( new Thread(){
            @Override
            public void run(){
                graphDb.shutdown();
            }
        } );
	}
	
	public List<String> readData(String fileName) throws IOException {
		String fileAllName = fileName;
		File file = new File(fileAllName); 
		InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
		BufferedReader br = new BufferedReader(reader); 
		List<String> list = new ArrayList<String>();
		String line = "";

		while((line = br.readLine()) != null) {
			list.add(line);  
		}
		for(int i = 0; i < list.size();i++) {
			System.out.println(list.get(i));
		}
		return list;
	}
	
	
	
	public int createDatabase(List<String> infos,String graph) throws IOException{
		myInsert=BatchInserters.inserter(new File(graphDatabase));
		Node[] nodes;
		int totalNumber = 0;
		// run the infos array
		for(int i = 0; i < infos.size();i++) {
			// create nodes
			if(i == 0) {
				totalNumber = Integer.parseInt(infos.get(i));
				nodes = new Node[totalNumber];
				//create node and save into the array 
				for(int j = 0; j < totalNumber; j++) {
					long ID = Long.valueOf(j);
					myInsert.createNode(ID, null, Label.label("NODE"));
					
				}
			}else {
				//start to set relationship
				String[] rela=infos.get(i).split(" ");
				long ID1 = Long.parseLong(rela[0]);
                long ID2 = Long.parseLong(rela[1]);
                myInsert.createRelationship(ID1, ID2, RelationshipType.withName(""), null);
				
			}
			
		}		
            myInsert.shutdown();
           // System.out.println("Create database successfully");
            return totalNumber;
    }
	
	public void pSCAN() {
		// initial similarDegree and effectiveDegree
		
		similarDegree = new int[nodeNumber];
		effectiveDegree = new int[nodeNumber];
//		min_cn = new int[nodeNumber];
		degree = new int[nodeNumber];
		// set sd = 0
		for(int i = 0; i < nodeNumber; i++) {
			similarDegree[i] = 0;
//			min_cn[i] = 0;
		}
		// set ed to full
		try (Transaction tx = model.beginTx()) {
			for(int i = 0; i < nodeNumber; i++) {
				Node node = model.getNodeById(Long.valueOf(i));
				int count = 0;
				Iterable<Relationship> it = node.getRelationships(Direction.INCOMING);
				for(Relationship re: it) {
					count++;
				}
				effectiveDegree[i] = count ;
				degree[i] = count ;
			}
			tx.success();
		}
		// line 5
		for(int u = 0; u < nodeNumber; u++) {
			checkCore(u);
			// if it is a core vertez
			if(similarDegree[u] >= miu) {
				clusterCore(u);
			}
		}
		
		
		


		
		
		
	}
	
	
	public void checkCore(int u) {
		if (effectiveDegree[u] > miu && similarDegree[u] < miu) {
			effectiveDegree[u] = degree[u];
			similarDegree[u] = 0;
			//get the neighbors
			ArrayList<Integer> neighbors = getNeighbors(u);
			for(Integer v:neighbors) {
				float similar = compute(u,v);
				similars.put(u + " " + v, similar);
				if(similar > eps) {
					similarDegree[u]++;
					
				}else {
					effectiveDegree[u]--;
				}
				
				//line 7
				if(!explored[v]) {
					if(similar > eps) {
						similarDegree[v]++;
					}else {
						effectiveDegree[v]--;
					}
				}
				if(effectiveDegree[u] < miu || similarDegree[u] >= miu) {
					
				}
	
			}
			explored[u] = true;
			
		}
	}
	
	public void clusterCore(int u) {
		//line 1 get N[u]`
		HashSet<Integer> neighborU_Cal = new HashSet<>();
		//get neighbors of u
		ArrayList<Integer> neighborU = getNeighbors(u);
		HashSet<Integer> setU = new HashSet<>(neighborU);
		for(Integer v:neighborU) {
			if (similars.containsKey(u +" " + v)) {
				neighborU_Cal.add(v);
			}
		}
		
		for(Integer v : neighborU_Cal) {
			if(similarDegree[v] >= miu && similars.get(u + " " + v) > eps) {
				union(u,v);
			}	
		}
		
		setU.removeAll(neighborU_Cal);
		for(Integer v:setU) {
			if(find(u) != find(v) && effectiveDegree[u] > miu) {
				float similar = compute(u,v);
				similars.put(u + " " + v, similar);
				if (!explored[v] ) {
					if (similar >= eps) {
						similarDegree[v]++;
					}else {
						effectiveDegree[u]--;
					}
				}
				if (similarDegree[v] >= miu && similar > eps) {
					union(u,v);
				}
			}
		}
		
		
	}
	
	public void union(int u, int v) {
		int indexU = find(u);
		int indexV = find(v);
		if (indexU != indexV) {
			saveNode nodeU = dis_joint.get(indexU);
			while(nodeU.next != null) {
				nodeU = nodeU.next;
			}
			nodeU.next = dis_joint.get(indexV);
		}
		dis_joint.remove(dis_joint.get(indexV));
		
	}
	
	public int find(int u) {
		for(int i = 0; i < dis_joint.size();i++) {
			saveNode node = dis_joint.get(i);
			while(node != null) {
				if(u == node.val) {
					return i;
				}
				node = node.next;
			}
			
		}
		return -1;
			
		
	}
	
	
	public float compute(int u, int v) {
		float res = 0.0f;
		try (Transaction tx = model.beginTx()) {
			ArrayList<Integer> neighborU = getNeighbors(u);
			ArrayList<Integer> neighborV = getNeighbors(v);
			
			neighborU.add(u);
			neighborV.add(v);
			
			// turn two array to set
			Set<Integer> setU = new HashSet<>(neighborU);
			Set<Integer> setV = new HashSet<>(neighborV);
			
			setU.retainAll(setV);
			
			res = (float)setU.size()/(float)(Math.sqrt(degree[u] * degree[v]));
		}
		
		
		
		return res;
	}
	
	
	
	public ArrayList<Integer> getNeighbors(int u){
		ArrayList<Integer> neighbors = new ArrayList<>();
		try (Transaction tx = model.beginTx()) {
			Node node = model.getNodeById(Long.valueOf(u));
			Iterable<Relationship> it = node.getRelationships(Direction.INCOMING);
			for(Relationship re: it) {
				neighbors.add((int)re.getOtherNode(node).getId());
			}
			tx.success();
		}
		return neighbors;
	} 
	
	
	public void PruneAndCrossLink() {
		try (Transaction tx = model.beginTx()) {
			for(int i = 0; i < nodeNumber; i++) {
//				long id = Long.valueOf(i);
				//find the node
				Node node = model.getNodeById(Long.valueOf(i));
				//create an array to save the neighbors
				ArrayList<Integer> neighbors = new ArrayList<>();
				//get all relationships
				Iterable<Relationship> it = node.getRelationships(Direction.INCOMING);
				for(Relationship re: it) {
					neighbors.add((int)re.getOtherNode(node).getId());
				}
				Collections.sort(neighbors);	
//				System.out.println(profiles);
//				String[]  a= profiles.toArray(new String[profiles.size()]);
//				int[] cur_neighbors = neighbors.toArray();
				
				for(int j = 0; j < neighbors.size();j++) {
					
					int v = neighbors.get(j);
//					if (v < i) {
//						if (min_cn[v] == 0) min_cn[j] = -2;
//						continue;// already checked
//					}
					int a = degree[i];
					int b = degree[v];
					
//					if(a > b) {
//						int temp = a;
//						a = b;
//						b = temp;
//					}
					if (a < eps * eps * b || b < eps* eps *  a) {
						
					}
					
					
				}
				
			}
		}

	}
	
	
	
	
	
}
class saveNode{
	public int val;
	public saveNode next;
	
	public saveNode(int val) {
		this.val = val;
		this.next = null;
	}
}



