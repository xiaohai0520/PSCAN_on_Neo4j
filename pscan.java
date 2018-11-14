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


	static String graphDatabase = "pscan_data";
	GraphDatabaseService model;
	BatchInserter myInsert;
	int nodeNumber;
	
	int[] degree;
	int[] similarDegree;//number of adjacent edges with similarity no less than epsilon
	int[] effectiveDegree;//number of adjacent edges not pruned by similarity
	
	float eps = (float) (2.0f/Math.sqrt(15));
	int miu = 4;
		
	boolean[] explored;
	ArrayList<saveNode> dis_joint;
	HashMap<String,Boolean> similars;
	ArrayList<saveNode> clusters;
	saveNode[] finalClusters;
	
	
	public pscan(String file,float eps, int miu) throws IOException {
		
		
		this.eps = eps;
		this.miu = miu;
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
	
//	public static void main(String[] args) throws IOException {
//		
//		pscan test = new pscan(file);
//		test.pSCAN();
////		System.out.println(test.dis_joint);
////		for(saveNode node : test.dis_joint) {
////			while(node != null) {
////				System.out.print(node.val);
////				node = node.next;
////			}
////			System.out.println(" ");
////		}
//		System.out.println(test.clusters);
//		for(int i = 0; i < test.finalClusters.length;i++) {
//			saveNode cluster = test.finalClusters[i];
//			while(cluster != null) {
//				System.out.print(cluster.val + " ");
//				cluster = cluster.next;
//			}
//		System.out.println(" ");
//		}
//	}
	
	
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
	
	public saveNode[] pSCAN() {
		// initial similarDegree and effectiveDegree
		
		similarDegree = new int[nodeNumber];
		effectiveDegree = new int[nodeNumber];
		degree = new int[nodeNumber];
		// set sd = 0
		for(int i = 0; i < nodeNumber; i++) {
			similarDegree[i] = 0;
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
				System.out.println(count + 1);
				effectiveDegree[i] = count + 1;
				degree[i] = count + 1;
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
		clusterNonCore();
	
		return finalClusters;
	}
	
	
	
	public void clusterNonCore() {
		// get the clusters and non-core
		HashSet<Integer> nonCores = new HashSet<>();
		clusters = new ArrayList<>();
		for(int i = 0; i < dis_joint.size();i++) {
			saveNode node = dis_joint.get(i);
			saveNode cur = node;
			//make sure the node is a cluster or a non core vertex
			int length = 0;
			while(cur != null) {
				cur = cur.next;
				length++;
			}
			//save the non core
			if(length == 1) {
				nonCores.add(node.val);
				//save the cluster
			}else if(length > 1) {
				
//				HashSet<Integer> cluster = new HashSet<>();
//				while(node != null) {
//					cluster.add(node.val);
//					node = node.next;
//				}
				clusters.add(node);
			}
		}
		// translate clusters from arraylist to array
		finalClusters = clusters.toArray(new saveNode[clusters.size()]);
		
		// now we have the cluster and non core 
		// just iterate each v belong to non core of each u in the cluster
		
		for(int i = 0; i < finalClusters.length;i++) {
			//get one cluster
			saveNode cluster = finalClusters[i];
			//iterate this cluster
//			cluster.iterator();
			
			while(cluster != null) {
				int u = cluster.val;
				ArrayList<Integer> neighbors = getNeighbors(u);
				for(Integer v: neighbors) {
					if (nonCores.contains(v)) {
						boolean isSimilar = checkStructureSimilar(u,v);
						if(isSimilar) {
							saveNode nonCore = new saveNode(v);
							nonCore.next = finalClusters[i];
							finalClusters[i] = nonCore;
						}
					}
				}
				cluster = cluster.next;
			}
			

		}
		
	}
	
	public void checkCore(int u) {
		if (effectiveDegree[u] >= miu && similarDegree[u] < miu) {
			effectiveDegree[u] = degree[u];
			similarDegree[u] = 0;
			//get the neighbors include itself
			ArrayList<Integer> neighbors = getNeighbors(u);
//			neighbors.add(u);
			System.out.println(u + ":" +neighbors);
			for(Integer v:neighbors) {
				//set a flag about similar
				Boolean isSimilar = false;
				if (similars.containsKey(u + " " + v)) {
					isSimilar = similars.get(u + " " + v);
				}else {
					isSimilar = checkStructureSimilar(u,v);
					similars.put(u + " " + v, isSimilar);
					similars.put(v + " " + u, isSimilar);
				}
				
//				float similar = compute(u,v);
//				System.out.println(u + "-" + v + ":" + similar);
				
				if(isSimilar) {
					similarDegree[u]++;	
					System.out.println(similarDegree[u]);
				}else {
					effectiveDegree[u]--;
				}
				
				//line 7
				if(!explored[v] && v != u) {
					if(isSimilar) {
						similarDegree[v]++;
					}else {
						effectiveDegree[v]--;
					}
				}
				// already to prove u whether is a core 
				if(effectiveDegree[u] < miu || similarDegree[u] >= miu) {
					break;
				}
	
			}
			
			
		}
		System.out.println("---------" + effectiveDegree[u] + ":" + similarDegree[u]);
		//mark already explore u
		explored[u] = true;
	}
	
	public void clusterCore(int u) {
		//line 1 get N[u]`
		HashSet<Integer> neighborU_Caled = new HashSet<>();
		//get neighbors of u
//		ArrayList<Integer> neighborU = ;
		
		HashSet<Integer> neighborU = new HashSet<>(getNeighbors(u));
//		System.out.println(u + "!!!!!!!" + neighborU);
		neighborU.remove(u);

		// need to remove u itself
//		neighborU.remove(u);
		System.out.println(u + "after" + neighborU);
		
		//find out already cal the similar degree neighbor of u
		for(Integer v:neighborU) {
			if (similars.containsKey(u +" " + v)) {
				neighborU_Caled.add(v);
			}
		}
		System.out.println(u + "+++++++++" + neighborU_Caled);
		
		for(Integer v : neighborU_Caled) {
			if(similarDegree[v] >= miu && similars.get(u + " " + v) ) {
				System.out.println("Union:" + u + " "+ v);
				union(u,v);
			}	
		}
		
		neighborU.removeAll(neighborU_Caled);
		for(Integer v:neighborU) {
			if(find(u) != find(v) && effectiveDegree[u] > miu) {
				Boolean isSimilar = checkStructureSimilar(u,v);
//				float similar = compute(u,v);
				similars.put(u + " " + v, isSimilar);
				similars.put(v + " " + u, isSimilar);
				if (!explored[v] ) {
					if (isSimilar) {
						similarDegree[v]++;
					}else {
						effectiveDegree[u]--;
					}
				}
				if (similarDegree[v] >= miu && isSimilar) {
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
			dis_joint.remove(dis_joint.get(indexV));
		}
		
		
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
			
			
			// turn two array to set
			Set<Integer> setU = new HashSet<>(neighborU);
			System.out.println(u + ":" + setU);
			Set<Integer> setV = new HashSet<>(neighborV);
			System.out.println(v + ":" + setV);
			setU.retainAll(setV);
			System.out.println( "after:" + setU);
			res = (float)setU.size()/(float)(Math.sqrt(degree[u] * degree[v]));
		}
		
		
		
		return res;
	}
	
	
	//get the neighbor of one vertex
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
		neighbors.add(u);
		return neighbors;
	} 
	
	
	public void PruneAndCrossLink() {
		//for each node
		for(int u = 0; u < nodeNumber;u++) {
			HashSet<Integer> neighborU = new HashSet<>(getNeighbors(u));
			for(Integer v:neighborU) {
				float eps_2 = eps * eps;
				if(degree[u] < eps_2 * degree[v] || degree[v] < eps_2 * degree[u] ) {
					similars.put(u + " " + v, false);
					effectiveDegree[u]--;
				}
				else {
					//cross link
				}
			}
		}

	}
	
	public boolean checkStructureSimilar(int u,int v){
		// get neighbors of u and v
		ArrayList<Integer> neighborU = getNeighbors(u);
		Collections.sort(neighborU);
		ArrayList<Integer> neighborV = getNeighbors(v);
		Collections.sort(neighborV);
		
		
		int min_cn = (int) Math.ceil(eps * Math.sqrt(degree[u] * degree[v]));
		int cn = 0, i = 0,j = 0;
		int du = degree[u],dv = degree[v];
		while ( cn < min_cn  && min_cn <= Math.min(du, dv) && i < degree[u] && j < degree[v]) {
			
			int nu = neighborU.get(i);
			int nv = neighborV.get(j);
			if (nu < nv) {
				du--;
				i++;
			}else if(nu > nv) {
				dv--;
				j++;
			}else {
				cn++;
				i++;
				j++;
			}
			
		}
		if (cn < min_cn) return false;
		
		return true;
		
		
	}

}



