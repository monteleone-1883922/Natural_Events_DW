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
FOREACH (_ IN CASE WHEN f1 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 5}]->(:County {fips_code: f1})
)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 6}]->(:County {fips_code: f2})
)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 7}]->(:County {fips_code: f3})
)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 8}]->(:County {fips_code: f4})
)
// add counties to existing traces
FOREACH (_ IN CASE WHEN f1 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 5}]->(:County {fips_code: f1})
)
FOREACH (_ IN CASE WHEN f2 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 6}]->(:County {fips_code: f2})
)
FOREACH (_ IN CASE WHEN f3 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 7}]->(:County {fips_code: f3})
)
FOREACH (_ IN CASE WHEN f4 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 8}]->(:County {fips_code: f4})
);