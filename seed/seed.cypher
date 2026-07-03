// Fallback bootstrap seed. This is only used if the graph has not yet been
// synced from real Azure DevOps data via prompts/sync_graph_from_azure_devops.md.
// Uses MERGE (not CREATE) so re-running this file is idempotent and safe to
// combine with agent-driven writes through mcp-neo4j-cypher.

MERGE (auth:Service {name: "AuthService"})
  ON CREATE SET auth.language = "Java", auth.buildTool = "Maven"

MERGE (frontend:Service {name: "Frontend"})
  ON CREATE SET frontend.language = "TypeScript", frontend.buildTool = "NPM"

MERGE (worker:Service {name: "DataWorker"})
  ON CREATE SET worker.language = "Python", worker.buildTool = "Pipenv"

MERGE (frontend)-[:DEPENDS_ON]->(auth)
MERGE (worker)-[:DEPENDS_ON]->(auth);
