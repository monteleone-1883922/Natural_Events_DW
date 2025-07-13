// crate States and Counties ---------------------
LOAD CSV WITH HEADERS FROM 'file:///counties.csv' AS row
MERGE (s:State {name: row.state_name,
                fips_code: toInteger(row.state_FIPS),
                area: row.area})
MERGE (c:County {name: row.county_name,
                 code: toInteger(row.county_code),
                 type: row.county_type,
                 fips_code: toInteger(row.county_FIPS),
                 gnis_code: toInteger(row.county_gnis_code),
                 full_name: row.county_full_name})
MERGE (c)-[ins: IN_STATE]->(s)
MERGE (s)-[hasc: HAS_COUNTY]->(c)


// create Tornadoes ----------------------------------
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row

WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg

WHERE (ns = 1 AND sn = 1 AND sg = 1) OR (ns <> 1 AND sn = 0 AND sg = 1)
// create tornado
CREATE (t:Tornado {id: toInteger(row.id),
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
// link to State
MERGE (t)-[:IN_STATE]->(:State {fips_code: toInteger(row.stf)})
// link to counties
FOREACH (_ IN CASE WHEN row.f1 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 1}]->(:County {fips_code: toInteger(row.f1)})
)
FOREACH (_ IN CASE WHEN row.f2 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 2}]->(:County {fips_code: toInteger(row.f2)})
)
FOREACH (_ IN CASE WHEN row.f3 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 3}]->(:County {fips_code: toInteger(row.f3)})
)
FOREACH (_ IN CASE WHEN row.f4 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 4}]->(:County {fips_code: toInteger(row.f4)})
)

// create traces ----------------------------------
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg
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
// link to State
MERGE (tr)-[:IN_STATE]->(:State {fips_code: toInteger(row.stf)})
// link to tornado
MERGE (t:Tornado {id: row.id})-[:TRACE {order_idx: CASE
WHEN row.slat = t.latitudeStart AND row.slon = t.longitudeStart THEN 1
WHEN row.elat = t.latitudeEnd AND row.elon = t.longitudeEnd THEN ns
ELSE 2
END}]->(tr)
// link to counties
FOREACH (_ IN CASE WHEN row.f1 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 1}]->(:County {fips_code: toInteger(row.f1)})
)
FOREACH (_ IN CASE WHEN row.f2 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 2}]->(:County {fips_code: toInteger(row.f2)})
)
FOREACH (_ IN CASE WHEN row.f3 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 3}]->(:County {fips_code: toInteger(row.f3)})
)
FOREACH (_ IN CASE WHEN row.f4 > 0 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 4}]->(:County {fips_code: toInteger(row.f4)})
)

// add missing counties to existing tornadoes and traces
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row
WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg
WHERE sg = -9
// add counties to existing tornadoes
FOREACH (_ IN CASE WHEN row.f1 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 5}]->(:County {fips_code: toInteger(row.f1)})
)
FOREACH (_ IN CASE WHEN row.f2 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 6}]->(:County {fips_code: toInteger(row.f2)})
)
FOREACH (_ IN CASE WHEN row.f3 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 7}]->(:County {fips_code: toInteger(row.f3)})
)
FOREACH (_ IN CASE WHEN row.f4 > 0 AND ns = 1 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 8}]->(:County {fips_code: toInteger(row.f4)})
)
// add counties to existing traces
FOREACH (_ IN CASE WHEN row.f1 > 0 AND ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 5}]->(:County {fips_code: toInteger(row.f1)})
)
FOREACH (_ IN CASE WHEN row.f2 > 0 ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 6}]->(:County {fips_code: toInteger(row.f2)})
)
FOREACH (_ IN CASE WHEN row.f3 > 0 ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 7}]->(:County {fips_code: toInteger(row.f3)})
)
FOREACH (_ IN CASE WHEN row.f4 > 0 ns <> 1 THEN [1] ELSE [] END |
  MERGE (tr)-[:AFFECTS_COUNTY {order_idx: 8}]->(:County {fips_code: toInteger(row.f4)})
);