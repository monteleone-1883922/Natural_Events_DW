CREATE RANGE INDEX trace_id_index
FOR (tra:Trace)
ON (tra.id);
// add missing counties to existing tornadoes and traces
LOAD CSV WITH HEADERS FROM 'file:///link_counties.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg,
     toInteger(row.f1) as f1,
     toInteger(row.f2) as f2,
     toInteger(row.f3) as f3,
     toInteger(row.f4) as f4,
     5 as i
// add counties to existing tornadoes
MATCH (s:State {fips_code: toInteger(row.stf)})
MATCH (t:Tornado {id: toInteger(row.id)})
OPTIONAL MATCH (ct1:County {fips_code: f1})-[:IN_STATE]->(s)
OPTIONAL MATCH (ict1:IndependentCity {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns = 1 AND ct1 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ct1)
)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns = 1 AND ct1 IS NULL AND ict1 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ict1)
)
// F2 ------------------------------------------
WITH t, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ct1 IS NOT NULL OR ict1 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (ct2:County {fips_code: f2})-[:IN_STATE]->(s)
OPTIONAL MATCH (ict2:IndependentCity {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 AND ct2 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ct2)
)
FOREACH (_ IN CASE WHEN f2 > 0 AND ct2 IS NULL AND ict2 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ict2)
)
// F3 ------------------------------------------
WITH t, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ct2 IS NOT NULL OR ict2 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (ct3:County {fips_code: f3})-[:IN_STATE]->(s)
OPTIONAL MATCH (ict3:IndependentCity {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns = 1 AND ct3 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ct3)
)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns = 1 AND ct3 IS NULL AND ict3 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ict3)
)
// F4 ------------------------------------------
WITH t, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ct3 IS NOT NULL OR ict3 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (ct4:County {fips_code: f4})-[:IN_STATE]->(s)
OPTIONAL MATCH (ict4:IndependentCity {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns = 1 AND ct4 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ct4)
)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns = 1 AND ct4 IS NULL AND ict4 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS {order_idx: i}]->(ict4)
)
// add counties to existing traces
WITH row, ns, sn, sg, f1, f2, f3, f4, s, 5 as j
MATCH (tr:Trace {id: toInteger(row.id)})
OPTIONAL MATCH (ctr1:County {fips_code: f1})-[:IN_STATE]->(s)
OPTIONAL MATCH (ictr1:IndependentCity {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns <> 1 AND ctr1 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ctr1)
)
FOREACH (_ IN CASE WHEN f1 > 0 AND ns <> 1 AND ctr1 IS NULL AND ictr1 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ictr1)
)
// F2 ------------------------------------------
WITH tr, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ctr1 IS NOT NULL OR ictr1 IS NOT NULL THEN j+1 ELSE j END as j
OPTIONAL MATCH (ctr2:County {fips_code: f2})-[:IN_STATE]->(s)
OPTIONAL MATCH (ictr2:IndependentCity {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns <> 1 AND ctr2 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ctr2)
)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns <> 1 AND ctr2 IS NULL AND ictr2 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ictr2)
)
// F3 ------------------------------------------
WITH tr, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ctr2 IS NOT NULL OR ictr2 IS NOT NULL THEN j+1 ELSE j END as j
OPTIONAL MATCH (ctr3:County {fips_code: f3})-[:IN_STATE]->(s)
OPTIONAL MATCH (ictr3:IndependentCity {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns <> 1 AND ctr3 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ctr3)
)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns <> 1 AND ctr3 IS NULL AND ictr3 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ictr3)
)
// F4 ------------------------------------------
WITH tr, row, f1, f2, f3, f4, s, ns, sn, sg, CASE WHEN ctr3 IS NOT NULL OR ictr3 IS NOT NULL THEN j+1 ELSE j END as j
OPTIONAL MATCH (ctr4:County {fips_code: f4})-[:IN_STATE]->(s)
OPTIONAL MATCH (ictr4:IndependentCity {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns <> 1 AND ctr4 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ctr4)
)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns <> 1 AND ctr4 IS NULL AND ictr4 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS {order_idx: j}]->(ictr4)
);