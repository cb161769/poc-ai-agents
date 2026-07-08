// Fallback bootstrap seed. This is only used if the graph has not yet been
// synced from real Azure DevOps data via prompts/sync_graph_from_azure_devops.md.
// Uses MERGE (not CREATE) so re-running this file is idempotent and safe to
// combine with agent-driven writes through mcp-neo4j-cypher.

// repo_url identifies which real git repo each component lives in -- used
// by the --epic mode (run_poc_loop.sh/orchestration.py) to detect whether
// an epic's children all live in the same repo before attempting a single
// combined run. The three components below share sample-repo/ as a
// monorepo of reference, hence the same repo_url; set the real one per
// component when syncing from Azure DevOps (see
// prompts/sync_graph_from_azure_devops.md).
MERGE (auth:Service {name: "AuthService"})
  ON CREATE SET auth.language = "Java", auth.buildTool = "Maven", auth.repo_url = "https://example.com/tuorg/sample-repo"

MERGE (frontend:Service {name: "Frontend"})
  ON CREATE SET frontend.language = "TypeScript", frontend.buildTool = "NPM", frontend.repo_url = "https://example.com/tuorg/sample-repo"

MERGE (worker:Service {name: "DataWorker"})
  ON CREATE SET worker.language = "Python", worker.buildTool = "Pipenv", worker.repo_url = "https://example.com/tuorg/sample-repo"

MERGE (frontend)-[:DEPENDS_ON]->(auth)
MERGE (worker)-[:DEPENDS_ON]->(auth);
