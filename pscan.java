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
	
	GraphDatabaseService model;
	BatchInserter myInsert;
	int nodeNumber;
	
	HashMap<Integer,Integer> degree;
	HashMap<Integer,Integer> similarDegree;//number of adjacent edges with similarity no less than epsilon
	HashMap<Integer,Integer> effectiveDegree;//number of adjacent edges not pruned by similarity
	
	float eps = 0.8f;
//	float eps = (float) (2.0f/Math.sqrt(15));
	int miu = 4;
		
	HashSet<Integer> explored;
	ArrayList<saveNode> dis_joint;
	HashMap<String,Boolean> similars;
	ArrayList<saveNode> clusters;
	saveNode[] finalClusters;
	
	// use for save all nodes
	HashSet<Integer> allNodes = new HashSet<>();
	public pscan(String file,String database,float eps, int miu) throws IOException {
		
		
		this.eps = eps;
		this.miu = miu;
		try {
			FileUtils.deleteRecursively( new File(database));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		List<String> datas = readData(file);
		nodeNumber = createDatabase(datas,database);
				
		model = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(database))
                .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
                .setConfig(GraphDatabaseSettings.string_block_size, "60")
                .setConfig(GraphDatabaseSettings.array_block_size, "300")
                .newGraphDatabase();
		
		registerShutdownHook( model );
		
		
		explored = new HashSet<>();
		dis_joint = new ArrayList<>();
		
		for(int id:allNodes) {
//			explored[i] = false;
			dis_joint.add(new saveNode(id));
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

//		read all nodes into the array
		while((line = br.readLine()) != null) {
//			System.out.println(line);
			if(line.substring(0, 1) == "#") {
				continue;
			}
			
			list.add(line);  
		}
//		for(int i = 0; i < list.size();i++) {
//			System.out.println(list.get(i));
//		}
		return list;
	}
	
	
	
	public int createDatabase(List<String> infos,String database) throws IOException{
		myInsert=BatchInserters.inserter(new File(database));
//		Node[] nodes;
//		HashSet<Long> saveNode = new HashSet<>();
		int totalNumber = 0;
		// run the infos array
		for(int i = 0; i < infos.size();i++) {
			// create nodes
//			if(i == 0) {
//				totalNumber = Integer.parseInt(infos.get(i));
//				nodes = new Node[totalNumber];
//				//create node and save into the array 
//				for(int j = 0; j < totalNumber; j++) {
//					long ID = Long.valueOf(j);
//					myInsert.createNode(ID, null, Label.label("NODE"));
//					
//				}
//			}else {
			
				//start to set relationship
			String[] rela=infos.get(i).split(" ");
//			System.out.println(infos.get(i));
			int ID1 = Integer.parseInt(rela[0]);
			int ID2 = Integer.parseInt(rela[1]);
			
//			long ID1 = Long.parseLong(rela[0]);
//            long ID2 = Long.parseLong(rela[1]);
            if(!allNodes.contains(ID1)) {
            		myInsert.createNode(Long.parseLong(rela[0]), null, Label.label("NODE"));
            		allNodes.add(ID1);
            		
            }
            
            if(!allNodes.contains(ID2)) {
        			myInsert.createNode(Long.parseLong(rela[1]), null, Label.label("NODE"));
        			allNodes.add(ID2);
            }	
            
            myInsert.createRelationship(Long.parseLong(rela[0]), Long.parseLong(rela[1]), RelationshipType.withName(""), null);
				
//			}
			
		}		
            myInsert.shutdown();
           // System.out.println("Create database successfully");
            return allNodes.size();
    }
	
	
	public void initStatus() {
		similarDegree = new HashMap<>();
		effectiveDegree = new HashMap<>();
		degree = new HashMap<>();
		
		try (Transaction tx = model.beginTx()) {
			
//			each node
			for(Integer id: allNodes) {
				Node node = model.getNodeById(Long.valueOf(id));
				int count = 0;
				Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
				for(Relationship re: it) {
					count++;
				}
				
				effectiveDegree.put(id, count+1);
				degree.put(id, count+1);
				similarDegree.put(id, 0);
			}
//			
//			
//			for(int i = 0; i < nodeNumber; i++) {
//				Node node = model.getNodeById(Long.valueOf(i));
//				int count = 0;
//				Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
//				for(Relationship re: it) {
//					count++;
//				}
//				
//				System.out.println(count + 1);
//				effectiveDegree[i] = count + 1;
//				degree[i] = count + 1;
//			}
			tx.success();
		}
		

	}
	
	
	public saveNode[] pSCAN() {
		// initial similarDegree and effectiveDegree
		initStatus();
//		similarDegree = new HashMap<Integer,Integer>();
//		effectiveDegree = new HashMap<Integer,Integer>();
//		degree = new HashMap<Integer,Integer>();
		// set sd = 0
//		for(Long id:allNodes) {
//			
//		}
//		for(int i = 0; i < nodeNumber; i++) {
//			
//			similarDegree[i] = 0;
//		}
		// set ed to full
//		try (Transaction tx = model.beginTx()) {
//			
//			for(int i = 0; i < nodeNumber; i++) {
//				Node node = model.getNodeById(Long.valueOf(i));
//				int count = 0;
//				Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
//				for(Relationship re: it) {
//					count++;
//				}
//				
//				System.out.println(count + 1);
//				effectiveDegree[i] = count + 1;
//				degree[i] = count + 1;
//			}
//			tx.success();
//		}
		// line 5
		for(int u: allNodes) {
			checkCore(u);
			
			if(similarDegree.get(u) >= miu) {
				clusterCore(u);
			}
			
		}
		System.out.println("Non core cluster: start");
		clusterNonCore();
		return finalClusters;
		
		
//		for(int u = 0; u < nodeNumber; u++) {
//			checkCore(u);
//			// if it is a core vertez
//			if(similarDegree[u] >= miu) {
//				
//			}
//		}
//		clusterNonCore();
	
		
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
				
				clusters.add(node);
			}
		}
		// translate clusters from arraylist to array
		finalClusters = clusters.toArray(new saveNode[clusters.size()]);
//		System.out.println(finalClusters);
//		System.out.println(nonCores);
		// now we have the cluster and non core 
		// just iterate each v belong to non core of each u in the cluster
		
		for(int i = 0; i < finalClusters.length;i++) {
			//get one cluster
			
			//iterate this cluster
//			cluster.iterator();
			
//			HashSet<Integer> totalNeighbors = new HashSet<>();
//			while (cluster != null) {
//				int u = cluster.val;
//				ArrayList<Integer> neighbors = getNeighbors(u);
//				for(int id : neighbors) {
//					totalNeighbors.add(id);
//				}
//				cluster = cluster.next;
//			}
//			for(int non_core: nonCores) {
//				if(total)
//			}
			for(int non_core:nonCores) {
//				System.out.println("+++++" + non_core);
				saveNode cluster = finalClusters[i];
				while(cluster != null) {
					int u = cluster.val;
					ArrayList<Integer> neighbors = getNeighbors(u);
					HashSet<Integer> neis = new HashSet<Integer>(neighbors);
					if(neis.contains(non_core)) {
						boolean isSimilar = checkStructureSimilar(u,non_core);
//						System.out.println(u +  "  " + non_core);
//						System.out.println("u and v:" + compute(u,non_core));
//						System.out.println(eps);
//						boolean isSimilar = compute(u,non_core) >= eps;
//						System.out.println(isSimilar);
						if(isSimilar) {
//							System.out.println("create" + non_core);
							saveNode cur = new saveNode(non_core);
							cur.next = finalClusters[i];
							finalClusters[i] = cur;
							break;
						}
					}
					cluster = cluster.next;
				}
				
			}
			
//			boolean flag = true;
//			while(cluster != null && flag) {
//				int u = cluster.val;
//				System.out.println(u);
//				ArrayList<Integer> neighbors = getNeighbors(u);
//				System.out.println(neighbors);
//				for(Integer v: neighbors) {
//					
//					if (nonCores.contains(v)) {
//						System.out.println(u + " " + v);
//						boolean isSimilar = checkStructureSimilar(u,v);
//						System.out.println(isSimilar);
//						if(isSimilar) {
//							saveNode nonCore = new saveNode(v);
//							nonCore.next = finalClusters[i];
//							finalClusters[i] = nonCore;
////							flag = false;
////							break;
//							
//						}
//					}
//				}
//				cluster = cluster.next;
//			}
			

		}
		
	}
	
	public void checkCore(int u) {
		if (effectiveDegree.get(u) >= miu && similarDegree.get(u) < miu) {
			effectiveDegree.put(u, degree.get(u));
//			effectiveDegree[u] = degree[u];
			similarDegree.put(u, 0);
//			similarDegree[u] = 0;
			//get the neighbors include itself
			ArrayList<Integer> neighbors = getNeighbors(u);
//			System.out.println(u + ":" +neighbors);
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
							
				if(isSimilar) {
					similarDegree.put(u,similarDegree.get(u)+1);
//					similarDegree[u]++;	
//					System.out.println(similarDegree[u]);
				}else {
					effectiveDegree.put(u,effectiveDegree.get(u)-1);
//					effectiveDegree[u]--;
				}
				
				//line 7
				if(!explored.contains(v) && v != u) {
					if(isSimilar) {
						similarDegree.put(v,similarDegree.get(v)+1);
//						similarDegree[v]++;
					}else {
						effectiveDegree.put(v,effectiveDegree.get(v)-1);
//						effectiveDegree[v]--;
					}
				}
				// already to prove u whether is a core 
				if(effectiveDegree.get(u) < miu || similarDegree.get(u) >= miu) {
					break;
				}
			}			
		}
//		System.out.println("---------" + effectiveDegree[u] + ":" + similarDegree[u]);
		//mark already explore u
//		explored[u] = true;
		explored.add(u);
	}
	
	public void clusterCore(int u) {
		//line 1 get N[u]`
		HashSet<Integer> neighborU_Caled = new HashSet<>();
		//get neighbors of u		
		HashSet<Integer> neighborU = new HashSet<>(getNeighbors(u));
		neighborU.remove(u);

		// need to remove u itself
//		neighborU.remove(u);
//		System.out.println(u + "after" + neighborU);
		
		//find out already cal the similar degree neighbor of u
		for(Integer v:neighborU) {
			if (similars.containsKey(u +" " + v)) {
				neighborU_Caled.add(v);
			}
		}
//		System.out.println(u + "+++++++++" + neighborU_Caled);
		
		for(Integer v : neighborU_Caled) {
			if(similarDegree.get(v) >= miu && similars.get(u + " " + v) ) {
//				System.out.println("Union:" + u + " "+ v);
				union(u,v);
			}	
		}
		
		neighborU.removeAll(neighborU_Caled);
		for(Integer v:neighborU) {
			if(find(u) != find(v) && effectiveDegree.get(u) > miu) {
				Boolean isSimilar = checkStructureSimilar(u,v);
//				float similar = compute(u,v);
				similars.put(u + " " + v, isSimilar);
				similars.put(v + " " + u, isSimilar);
				if (!explored.contains(v) ) {
					if (isSimilar) {
//						similarDegree[v]++;
						similarDegree.put(v, similarDegree.get(v)+1);
					}else {
						effectiveDegree.put(u, effectiveDegree.get(u)-1);
//						effectiveDegree[u]--;
					}
				}
				if (similarDegree.get(v) >= miu && isSimilar) {
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
			res = (float)setU.size()/(float)(Math.sqrt(degree.get(u) * degree.get(v)));
		}
		
		
		
		return res;
	}
	
	
	//get the neighbor of one vertex
	public ArrayList<Integer> getNeighbors(int u){
		ArrayList<Integer> neighbors = new ArrayList<>();
		try (Transaction tx = model.beginTx()) {
			Node node = model.getNodeById(Long.valueOf(u));
			Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
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
				if(degree.get(u) < eps_2 * degree.get(v) || degree.get(v) < eps_2 * degree.get(u) ) {
					similars.put(u + " " + v, false);
					effectiveDegree.put(u, effectiveDegree.get(u)-1);
//					effectiveDegree[u]--;
				}
//				else {
//					//cross link
//				}
			}
		}

	}
	
	public boolean checkStructureSimilar(int u,int v){
		// get neighbors of u and v
		ArrayList<Integer> neighborU = getNeighbors(u);
		Collections.sort(neighborU);
//		System.out.println(neighborU);
		ArrayList<Integer> neighborV = getNeighbors(v);
		Collections.sort(neighborV);
//		System.out.println(neighborV);
//		System.out.println(degree.get(u) + " " + degree.get(v));
		int min_cn = (int) Math.ceil(eps * Math.sqrt(degree.get(u) * degree.get(v)));
//		System.out.println(min_cn);
		int cn = 0, i = 0,j = 0;
		int du = degree.get(u),dv = degree.get(v);
		while ( cn < min_cn  && min_cn <= Math.min(du, dv) && i < degree.get(u) && j < degree.get(v)) {
			
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



