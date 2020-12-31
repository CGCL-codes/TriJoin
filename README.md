
./storm jar ../target/Trijoin-1.0-SNAPSHOT.jar com.basic.core.Topology -n 90 -pr 45 -ps 45 -pt 45 -sf 8 -dp 8 -win -wl 2000 --remote --s random
 
-pr -ps  -pt: partitions of relation R S T
-sf -dp: instances of shuffle dispatcher
-win：enable sliding window
-wl：join window length in ms  

-ba: enable barrier  
--barrier-period: barrier period
--remote：run topology in cluster (remote mode)
--s: partion scheme
