// crate States and Counties ---------------------
LOAD CSV WITH HEADERS FROM 'file:///independent_cities.csv' AS row
WITH row
WHERE row.county_name IS NULL AND row.county_fips IS NOT NULL
CREATE (ic:IndependentCity {name: row.city_name,
                            fips_code: toInteger(row.city_fips),
                            region : row.city_region})
// link to state
WITH row, ic
MATCH (s:State {fips_code: toInteger(row.state_fips)})
MERGE (ic)-[:IN_STATE]->(s)
// link to county
WITH row, s, ic
OPTIONAL MATCH (c1:County {fips_code: toInteger(row.county_fips)})-[:IN_STATE]->(s)
OPTIONAL MATCH (ic1:IndependentCity {fips_code: toInteger(row.county_fips)})-[:IN_STATE]->(s)
FOREACH (_ IN CASE WHEN c1 IS NOT NULL THEN [1] ELSE [] END |
  MERGE (ic)-[:IN_COUNTY]->(c1)
)
FOREACH (_ IN CASE WHEN c1 IS NULL THEN [1] ELSE [] END |
  MERGE (ic)-[:IN_CITY]->(ic1)
)
;
