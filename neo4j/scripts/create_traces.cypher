CREATE RANGE INDEX tornado_id_index
FOR (tor:Tornado)
ON (tor.id);
// create traces ----------------------------------
LOAD CSV WITH HEADERS FROM 'file:///traces.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg,
     toInteger(row.f1) as f1,
     toInteger(row.f2) as f2,
     toInteger(row.f3) as f3,
     toInteger(row.f4) as f4
// create tornado trace
CREATE (tr:Trace {id: toInteger(row.id),
            tornado_id: toInteger(row.tornado_id),
            year: toInteger(row.yr),
            month: toInteger(row.mo),
            day: toInteger(row.dy),
            date: date(row.date),
            time: time(row.time),
            timeZone: toInteger(row.tz),
            stateNumber: toInteger(row.stn),
            f_scale: toInteger(row.mag),
            injuries: toInteger(row.inj),
            fatalities: toInteger(row.fat),
            millionsDollarsDamage: toFloat(row.loss),
            millionDollarsCropsDamage: toFloat(row.closs),
            latitudeStart: toFloat(row.slat),
            longitudeStart: toFloat(row.slon),
            latitudeEnd: toFloat(row.elat),
            longitudeEnd: toFloat(row.elon),
            length: toInteger(row.len),
            width: toInteger(row.wid),
            alteredMagnitude: toInteger(row.fc)
})
WITH tr, row, f1, f2, f3, f4, ns
// link to State
MATCH (s:State {fips_code: toInteger(row.stf)})
CREATE (tr)-[:IN_STATE]->(s)
WITH tr, row, f1, f2, f3, f4, ns, s
// link to tornado
MATCH (t:Tornado {id: toInteger(row.tornado_id)})
CREATE (t)-[:TRACE {order_idx: CASE
WHEN abs(toFloat(row.slat) - t.latitudeStart) < 0.0001 AND abs(toFloat(row.slon) - t.longitudeStart) < 0.0001 THEN 1
WHEN abs(toFloat(row.elat) - t.latitudeEnd) < 0.0001 AND abs(toFloat(row.elon) - t.longitudeEnd) < 0.0001 THEN ns
ELSE 2
END}]->(tr)
// link to counties
WITH tr, row, f1, f2, f3, f4, s, 1 as i
OPTIONAL MATCH (c1:County {fips_code: f1})-[:IN_STATE]->(s)
OPTIONAL MATCH (ic1:IndependentCity {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 AND c1 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(c1)
)
FOREACH (_ IN CASE WHEN f1 > 0 AND c1 IS NULL AND ic1 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(ic1)
)
// F2 -------------------------------------
WITH tr, row, f1, f2, f3, f4, s, CASE WHEN c1 IS NOT NULL OR ic1 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (c2:County {fips_code: f2})-[:IN_STATE]->(s)
OPTIONAL MATCH (ic2:IndependentCity {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 AND c2 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(c2)
)
FOREACH (_ IN CASE WHEN f2 > 0 AND c2 IS NULL AND ic2 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(ic2)
)
// F3 -------------------------------------
WITH tr, row, f1, f2, f3, f4, s, CASE WHEN c2 IS NOT NULL OR ic2 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (c3:County {fips_code: f3})-[:IN_STATE]->(s)
OPTIONAL MATCH (ic3:IndependentCity {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 AND c3 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(c3)
)
FOREACH (_ IN CASE WHEN f3 > 0 AND c3 IS NULL AND ic3 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(ic3)
)
// F4 -------------------------------------
WITH tr, row, f1, f2, f3, f4, s, CASE WHEN c3 IS NOT NULL OR ic3 IS NOT NULL THEN i+1 ELSE i END as i
OPTIONAL MATCH (c4:County {fips_code: f4})-[:IN_STATE]->(s)
OPTIONAL MATCH (ic4:IndependentCity {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 AND c4 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(c4)
)
FOREACH (_ IN CASE WHEN f4 > 0 AND c4 IS NULL AND ic4 IS NOT NULL THEN [1] ELSE [] END |
  CREATE (tr)-[:AFFECTS {order_idx: i}]->(ic4)
);