graph1 = LOAD '$G' USING PigStorage(',') as (node_no: int, neighbour_node_no: int);
graph1_grouped = GROUP graph1 by node_no;
graph1_stage = foreach graph1_grouped generate group, COUNT(graph1);
graph2 = foreach graph1_stage generate $1 as neighbour_nodes_count, $0 as node_no;
graph2_grouped = GROUP graph2 by neighbour_nodes_count;
graph2_stage = foreach graph2_grouped generate group, COUNT(graph2);
STORE graph2_stage INTO '$O' USING PigStorage(' ');
