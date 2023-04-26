# TriJoin

TriJoin is a high-efficiency and scalable three-way steam join system. Stream join applications usually use three-way join, which can be used to stream Multi-join.
## How to use?

### Environment

We implement TriJoin on top of Apache Storm (version 1.2.3 or higher), and deploy the system on a cluster. Each machine is equipped with an octa-core 2.4GHz Xeon CPU, 64.0GB RAM, and a 1000Mbps Ethernet interface card. One machine in the cluster serves as the master node to host the Storm Nimbus. The other machines run Storm supervisors.

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

