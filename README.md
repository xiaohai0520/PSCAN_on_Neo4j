# pSCAN - Fast and Exact Structural Graph Clustering (with overlaps on Neo4j)

The paper: *["pSCAN: Fast and Exact Structural Graph Clustering"](https://www.cse.unsw.edu.au/~ljchang/pdf/icde16-pscan.pdf) by Lijun Chang, Wei Li, Xuemin Lin,Lu Qin and Wenjie Zhang, ICDE'16*.

This is the refactored version of the pScan with Java on the Neo4j.

## Content
- [Deployment](#deployment)
- [Usage](#usage)
  - [Input](#input)
  - [Output](#output)
- [Reference Projects](#related-projects)

# Deployment

The program depends on the graph database Neo4j. *[Here is the link for Neo4j on the github](https://github.com/neo4j/neo4j)*


# Usage

```
Clusters unweighted undirected input network (graph) considering overlaps and
building Exact Structural Graph

Usage: RUN_PSCAN [OPTIONS]... [input_network]...

input_network  - the input graph specified as a file in the TXT type.

  -inputnetwork        Path of the data file.
  -outputnetwork       Mode of the graph database.
  -eps                 The threshold of the similar for the clusters.
  -miu                 The least number for the clusters.
```
For example
```
$java RUN_PSCAN ./data/data.txt ./model 0.5 4

```

## Input
The undirected unweighted input network to be clustered is specified in the TXT format files:


	```
	# Example Network
	# Nodes number  
    9
	# Note that the links in the database
	0 1
	0 2
	2 1
	```
## Output
The CNL (clusters nodes list) output is a standard format. For example:
```
# Clusters: 2, Nodes: 9
0
1 3 2 4
```  
# Reference Projects
- [pSCAN](https://github.com/LijunChang/pSCAN) - pSCAN: Fast and Exact Structural Graph Clustering.

