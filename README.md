
./storm jar ../target/Trijoin-1.0-SNAPSHOT.jar com.basic.core.Topology -n 90 -ks 5 -pr 45 -ps 45 -sf 8 -dp 8 -dr 40 -win -wl 2000 -cd --remote --s random

-ks: spout实例  
-pr -ps: R S实例  
-sf -dp: shuffle dispatcher实例  
-win：开启窗口join  
-wl：窗口长度（ms）  
  
-dr: 检查结果是否重复实例  
-cd: 开启重复结果检查（不开的话就不会起DuplicateBolt）  

-ba: 开启barrier  
--barrier-period: barrier周期  
--remote：远程执行  
