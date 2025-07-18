// add missing counties to existing tornadoes and traces
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg,
     toInteger(row.f1) as f1,
     toInteger(row.f2) as f2,
     toInteger(row.f3) as f3,
     toInteger(row.f4) as f4
WHERE sg = -9
// add counties to existing tornadoes
MATCH (s:State {fips_code: toInteger(row.stf)})
OPTIONAL MATCH (ct1:County {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 5}]->(ct1)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ct2:County {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 6}]->(ct2)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ct3:County {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 7}]->(ct3)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ct4:County {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 8}]->(ct4)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
// add counties to existing traces
OPTIONAL MATCH (ctr1:County {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 5}]->(ctr1)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ctr2:County {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 6}]->(ctr2)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ctr3:County {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 7}]->(ctr3)
)
WITH row, ns, sn, sg, f1, f2, f3, f4, s
OPTIONAL MATCH (ctr4:County {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 8}]->(ctr4)
);