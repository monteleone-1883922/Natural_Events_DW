// crate States and Counties ---------------------
LOAD CSV WITH HEADERS FROM 'file:///counties.csv' AS row
MERGE (s:State {name: row.state_name,
                fips_code: toInteger(row.state_FIPS),
                area: row.area})
MERGE (c:County {name: row.county_name,
                 code: toInteger(row.county_code),
                 type: COALESCE(row.county_type, "unknown"),
                 fips_code: toInteger(row.county_FIPS),
                 gnis_code: toInteger(row.county_gnis_code),
                 full_name: row.county_full_name})
MERGE (c)-[ins: IN_STATE]->(s)
;