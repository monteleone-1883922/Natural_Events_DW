// crate States and Counties ---------------------
LOAD CSV WITH HEADERS FROM 'file:///big_independent_cities.csv' AS row
WITH row
WHERE row.county_name IS NULL AND row.county_fips IS NULL
CREATE (ic:IndependentCity {name: row.city_name,
                            fips_code: toInteger(row.city_fips),
                            region : row.city_region})
// link to state
WITH row, ic
MATCH (s:State {fips_code: toInteger(row.state_fips)})
MERGE (ic)-[:IN_STATE]->(s)
