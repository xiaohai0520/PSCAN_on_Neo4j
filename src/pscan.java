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
import java.util.TreeSet;
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
	
	float eps;
	int miu;
	
	HashSet<Integer> explored;

	ArrayList<Set<Integer>> dis_joint;
	HashMap<String,Boolean> similars;

	saveNode[] finalClusters;
	
	ArrayList<Set<Integer>> clusters;
	
	// use for save all nodes
	Set<Integer> allNodes;
	
	public pscan(String file,String database,float eps, int miu) throws IOException {
		
		
		this.eps = eps;
		this.miu = miu;
		try {
			FileUtils.deleteRecursively( new File(database));
		} catch (IOException e) {
			e.printStackTrace();
		}
		allNodes = new TreeSet<>();
		
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
//		dis_joint = new ArrayList<>();
		dis_joint = new ArrayList<Set<Integer>>();
		clusters = new ArrayList<Set<Integer>>();
		for(int id:allNodes) {

			Set<Integer> cur = new TreeSet<Integer>();
			cur.add(id);
			dis_joint.add(cur);
//			dis_joint.add(new saveNode(id));
		}
//		System.out.println("All nodes:\n" + dis_joint + "\n");
		
		similars = new HashMap<>();
		
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
//			System.out.println(infos.get(i));
				//start to set relationship
			String[] rela=infos.get(i).split("\\s+");
//			System.out.println(rela[0]);
//			System.out.println(rela[1]);
//			System.out.println(infos.get(i));
			int ID1 = Integer.parseInt(rela[0]);
			int ID2 = Integer.parseInt(rela[1]);
			
//			make sure whether have been created
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
//            System.out.println(allNodes.size());

            return allNodes.size();
    }
	
	
	public void initStatus() {
		similarDegree = new HashMap<>();
		effectiveDegree = new HashMap<>();
		degree = new HashMap<>();
		
		try (Transaction tx = model.beginTx()) {
			
//			each node
			for(Integer id: allNodes) {
//				Node node = model.getNodeById(Long.valueOf(id));
//				
//				Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
//				
//				Set<Long> cur = new HashSet<Long>();
//				Long l_id = Long.valueOf(id);
//				for(Relationship re: it) {
//					cur.add(re.getOtherNodeId(l_id));
//				}
//				
//				cur.add(l_id);
				Set<Integer> cur = getNeighbors(id);
				
				effectiveDegree.put(id, cur.size());
//				System.out.println(l_id + ":" + cur.size());
				degree.put(id, cur.size());
				similarDegree.put(id, 0);
			}

			tx.success();
		}
		

	}
	
	
	public List<Set<Integer>> pSCAN() {
		// initial similarDegree and effectiveDegree and degree
		
		initStatus();
//		PruneAndCrossLink();
		// line 5
		System.out.println("start check core");
		for(int u: allNodes) {
//			System.out.println("check:" + u);
			// check core vertex for u
			checkCore(u);
			
			if(similarDegree.get(u) >= miu) {
				clusterCore(u);
			}
			
		}
//		System.out.println(dis_joint);
		System.out.println("Non core cluster: start");
		clusterNonCore();
		model.shutdown();
		return clusters;
		
	
		
	}
	
	
	
	public void clusterNonCore() {
		// get the clusters and non-core
		
		Set<Integer> nonCores = new HashSet<>();
		clusters = new ArrayList<Set<Integer>>();
		for(int i = 0; i < dis_joint.size();i++) {
			Set<Integer> cur = dis_joint.get(i);
			if(cur.size() > 1) {
				clusters.add(cur);
			}else {
				nonCores.add(cur.iterator().next());
			}
		}
		System.out.println("Core clusters:\n" + clusters + "\n");
		System.out.println("non Cores:\n" + nonCores + "\n");

		for(Set<Integer> cluster : clusters) {
			for(Integer nonCore: nonCores) {
				for (Integer u : cluster) {
					Set<Integer> neighbors = getNeighbors(u);
					if(neighbors.contains(nonCore)) {
						boolean isSimilar = checkStructureSimilar(u,nonCore);
//						boolean isSimilar = compute(u,nonCore);
						if(isSimilar) {
							cluster.add(nonCore);
							break;
						}
						
					}
				}
			}
		}
		
//		System.out.println(clusters);
	}	
	
	public void checkCore(int u) {
		if (effectiveDegree.get(u) >= miu && similarDegree.get(u) < miu) {
			effectiveDegree.put(u, degree.get(u));
//			effectiveDegree[u] = degree[u];
			similarDegree.put(u, 0);
//			similarDegree[u] = 0;
			//get the neighbors include itself
			Set<Integer> neighbors = getNeighbors(u);
//			System.out.println(u + ":" +neighbors);
			for(Integer v:neighbors) {
				//set a flag about similar
				Boolean isSimilar = false;
				if (similars.containsKey(u + " " + v)) {
					isSimilar = similars.get(u + " " + v);
				}else {
					isSimilar = checkStructureSimilar(u,v);
//					isSimilar = compute(u,v);
					similars.put(u + " " + v, isSimilar);
					//similars.put(v + " " + u, isSimilar);
				}
//				if (u == 0) {
//					System.out.println( u + ":" + v + " " + isSimilar);
//				}
							
				if(isSimilar) {
					similarDegree.put(u,similarDegree.get(u)+1);
				}else {
					effectiveDegree.put(u,effectiveDegree.get(u)-1);
				}
				
				//line 7
				if(!explored.contains(v) && v != u) {
					if(isSimilar) {
						similarDegree.put(v,similarDegree.get(v)+1);

					}else {
						effectiveDegree.put(v,effectiveDegree.get(v)-1);

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
//		if(u == 0) {
//			System.out.println("ed:" + effectiveDegree.get(u) + " sd:" + similarDegree.get(u));	
//		}
		explored.add(u);
	}
	
	public void clusterCore(int u) {
		//line 1 get N[u]`
		Set<Integer> neighborU_Caled = new HashSet<>();
		//get neighbors of u		
		Set<Integer> neighborU = getNeighbors(u);


		// need to remove u itself
		neighborU.remove(u);
		
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
//				Boolean isSimilar = compute(u,v);
				similars.put(u + " " + v, isSimilar);
				similars.put(v + " " + u, isSimilar);
				if (!explored.contains(v) ) {
					if (isSimilar) {

						similarDegree.put(v, similarDegree.get(v)+1);
					}else {
						effectiveDegree.put(u, effectiveDegree.get(u)-1);

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
			Set<Integer> cluster_u = dis_joint.get(indexU);
			Set<Integer> cluster_v = dis_joint.get(indexV);
			
			cluster_u.addAll(cluster_v);
			
			dis_joint.remove(indexV);
		}
		
		
	}
	
	public int find(int u) {
		for(int i = 0; i < dis_joint.size();i++) {
			Set<Integer> cur = dis_joint.get(i);
			if(cur.contains(u)) {
				return i;
			}			
		}
		return -1;
			
		
	}
	
	
	public boolean compute(int u, int v) {
		float res = 0.0f;
		try (Transaction tx = model.beginTx()) {
			Set<Integer> neighborU = getNeighbors(u);
			
			Set<Integer> neighborV = getNeighbors(v);
			
			
			// turn two array to set
//			Set<Integer> setU = new HashSet<>(neighborU);
//			System.out.println(u + ":" + setU);
//			Set<Integer> setV = new HashSet<>(neighborV);
//			System.out.println(v + ":" + setV);
			neighborU.retainAll(neighborV);
//			System.out.println( "after:" + neighborV);
			res = (float)neighborU.size()/(float)(Math.sqrt(degree.get(u) * degree.get(v)));
		}
		
		
		
		return res >= eps;
	}
	
	
	//get the neighbor of one vertex
	public Set<Integer> getNeighbors(int u){
		
		Set<Integer> neighbors = new TreeSet<>();
		
		try (Transaction tx = model.beginTx()) {
			Long l_id = Long.valueOf(u);
			Node node = model.getNodeById(l_id);
			Iterable<Relationship> it = node.getRelationships(Direction.BOTH);
			for(Relationship re: it) {
				neighbors.add((int)re.getOtherNodeId(l_id));
			}
			tx.success();
		}
		neighbors.add(u);
//		System.out.println(u +":nei:" + neighbors);
		return neighbors;
	} 
	
	
	public void PruneAndCrossLink() {
		//for each node
		float eps_2 = eps * eps;
		for(Integer u: allNodes) {
			Set<Integer> neighborU = getNeighbors(u);
			for(Integer v:neighborU) {
				if(!similars.containsKey(u + " " + v)) {
					if(degree.get(u) < eps_2 * degree.get(v) || degree.get(v) < eps_2 * degree.get(u) ) {
						similars.put(u + " " + v, false);				
						effectiveDegree.put(u, effectiveDegree.get(u)-1);
						
//						build cross link
						similars.put(v + " " + u, false);				
						effectiveDegree.put(v, effectiveDegree.get(v)-1);

					}
				}

			}
		}

	}
	
	
	
	
	
	
	public boolean checkStructureSimilar(int u,int v){
		// get neighbors of u and v
		Set<Integer> neighborU_cur = getNeighbors(u);
		List<Integer> neighborU = new ArrayList<>();
		neighborU.addAll(neighborU_cur);
//		Collections.sort(neighborU);
//		System.out.println(neighborU);
		Set<Integer> neighborV_cur = getNeighbors(v);
		List<Integer> neighborV = new ArrayList<>();
		neighborV.addAll(neighborV_cur);
//		Collections.sort(neighborV);
//		System.out.println(neighborV);
//		System.out.println(degree.get(u) + " " + degree.get(v));
		int min_cn = (int) Math.ceil(eps * Math.sqrt(degree.get(u) * degree.get(v)));
//		System.out.println(min_cn);
		int cn = 0, i = 0,j = 0;
		int du = degree.get(u),dv = degree.get(v);
		
//		System.out.println(u + ":" + v);
//		System.out.println(neighborV.size());
		while ( cn < min_cn  && min_cn <= Math.min(du, dv) && i < degree.get(u) && j < degree.get(v)) {
//			System.out.println(j + "!!!!!!!");
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



