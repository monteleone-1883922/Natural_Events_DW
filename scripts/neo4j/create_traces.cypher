// create traces ----------------------------------
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg,
     toInteger(row.f1) as f1,
     toInteger(row.f2) as f2,
     toInteger(row.f3) as f3,
     toInteger(row.f4) as f4
WHERE ns <> 1 AND sn = 1 AND sg = 2
// create tornado trace
CREATE (tr:Trace {id: toInteger(row.id),
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
MERGE (tr)-[:IN_STATE]->(s)
WITH tr, row, f1, f2, f3, f4, ns, s
// link to tornado
MATCH (t:Tornado {id: toInteger(row.id)})
MERGE (t)-[:TRACE {order_idx: CASE
WHEN row.slat = t.latitudeStart AND row.slon = t.longitudeStart THEN 1
WHEN row.elat = t.latitudeEnd AND row.elon = t.longitudeEnd THEN ns
ELSE 2
END}]->(tr)
WITH tr, row, f1, f2, f3, f4, s
// link to counties
OPTIONAL MATCH (c1:County {fips_code: f1})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f1 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 1}]->(c1)
)
WITH tr, row, f1, f2, f3, f4, s
OPTIONAL MATCH (c2:County {fips_code: f2})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f2 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 2}]->(c2)
)
WITH tr, row, f1, f2, f3, f4, s
OPTIONAL MATCH (c3:County {fips_code: f3})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f3 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 3}]->(c3)
)
WITH tr, row, f1, f2, f3, f4, s
OPTIONAL MATCH (c4:County {fips_code: f4})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN f4 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 4}]->(c4)
);