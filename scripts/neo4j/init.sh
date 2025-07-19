if [ -f /init/.seed_done ]; then
  echo 'Seed already done. Skipping.';
  exit 0;
fi
until cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 'RETURN 1'; do
  echo 'Waiting for Neo4j to be ready...';
  sleep 3;
done;
echo 'Creating Counties';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_counties.cypher;
echo 'Creating Big Independent Cities';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_big_independent_cities.cypher;
echo 'Creating Independent Cities';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_independent_cities.cypher;
echo 'Creating Tornadoes';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_tornadoes.cypher;
echo 'Creating Traces';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_traces.cypher;
echo 'Creating new links to Counties';
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password1234 -f /init/create_links_counties.cypher;
echo 'Finished seeding the database.';
touch /init/.seed_done;