# TriJoin

TriJoin is a high-efficiency and scalable three-way steam join system. In order to reduce the  processing  latency,  we  idealize  a  latency  model,  and design the TriJoin according to the idealized latency model. In view  of  different  join  conditions,  we  design  different  stream data  partition  and  join  processing  scheme.  For  general  high-selectivity join, which is when the combination of all or most of  the  tuples  in  the  three  steams  may  produce  join  results, TriJoin adopts a hybrid partition scheme to ensure any tuples in  the  three  streams  completes  exactly  one  join  operation; for low-selectivity join such as equi-join, TriJoin employs an optimized hash partitioning scheme. This scheme can route alltuples  with  the  same  key  to  the  same  processing  unit,  which reduces  the  unnecessary  cost  of  join  operations  and storage. In  order  to  ensure  the  high  timeliness  of  the  system,  Tri-Join correspondingly designed an efficient intermediate resultstorage  structure,  thereby  reducing  the  repeated  operation  of intermediate results and significantly reducing the processing latency. 

## How to use?

### Environment

We implement Simois atop Apache Storm (version 1.2.3 or higher), and deploy the system on a cluster. Each machine is equipped with an octa-core 2.4GHz Xeon CPU, 64.0GB RAM, and a 1000Mbps Ethernet interface card. One machine in the cluster serves as the master node to host the Storm Nimbus. The other machines run Storm supervisors.

Build and run the example

```txt
mvn clean package

./storm jar ../target/Trijoin-1.0-SNAPSHOT.jar com.basic.core.Topology -n 90 -pr 45 -ps 45 -pt 45 -sf 8 -dp 8 -win -wl 2000 --remote --s random
```

-pr -ps  -pt: partitions of relation R S T

-sf -dp: instances of shuffle dispatcher

-win：enable sliding window

-wl：join window length in ms  

-ba: enable barrier  

--barrier-period: barrier period

--remote：run topology in cluster (remote mode)

--s: partion scheme

