
// create Tornadoes ----------------------------------
LOAD CSV WITH HEADERS FROM 'file:///tornado.csv' AS row

WITH row,
     toInteger(row.ns) as ns,
     toInteger(row.sn) as sn,
     toInteger(row.sg) as sg,
     toInteger(row.f1) as f1,
     toInteger(row.f2) as f2,
     toInteger(row.f3) as f3,
     toInteger(row.f4) as f4

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
FOREACH (_ IN CASE WHEN f1 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 1}]->(:County {fips_code: f1})
)
FOREACH (_ IN CASE WHEN f2 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 2}]->(:County {fips_code: f2})
)
FOREACH (_ IN CASE WHEN f3 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 3}]->(:County {fips_code: f3})
)
FOREACH (_ IN CASE WHEN f4 > 0 THEN [1] ELSE [] END |
  MERGE (t)-[:AFFECTS_COUNTY {order_idx: 4}]->(:County {fips_code: f4})
);