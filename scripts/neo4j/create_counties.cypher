// crate States and Counties ---------------------
LOAD CSV WITH HEADERS FROM 'file:///counties.csv' AS row
WITH row, linenumber() AS incremental_id
MERGE (s:State {name: row.state_name,
                fips_code: toInteger(row.state_fips)})
MERGE (c:County {name: row.county_name,
                 fips_code: toInteger(row.county_fips),
                 id : incremental_id})
MERGE (c)-[:IN_STATE]->(s)
;