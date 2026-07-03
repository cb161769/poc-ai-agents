# Prompt: sincronizar el grafo de dependencias desde Azure DevOps real

Pega este prompt en Copilot Chat (VS Code) con los servidores MCP
`azure-devops` y `neo4j-cypher` activos (ver `.vscode/mcp.json`). Reemplaza
esto al `seed.cypher` estático como fuente de verdad del grafo.

---

Tienes acceso a dos herramientas MCP: `azure-devops` (repos y work items
reales de mi organización) y `neo4j-cypher` (lectura/escritura sobre un
grafo Neo4j local).

1. Usa las tools de `azure-devops` para listar los repositorios del proyecto
   que corresponde a este PoC (busca repos llamados o relacionados con
   `auth-service`, `frontend`, `data-worker`; si los nombres reales
   difieren, úsalos tal cual existen en la organización).
2. Para cada repo, inspecciona su manifiesto de dependencias
   (`pom.xml`, `package.json`, `Pipfile`) para confirmar qué otros
   repos/servicios internos referencia.
3. Usando `neo4j-cypher` (`write_neo4j_cypher`), aplica un `MERGE` — nunca
   `CREATE` — por cada nodo `:Service {name, language, buildTool}` y cada
   relación `:DEPENDS_ON`, para que la operación sea idempotente si la
   vuelvo a correr después.
4. Al final, confirma con `read_neo4j_cypher` (`MATCH (n) RETURN n.name,
   n.language`) que el grafo resultante coincide con lo que encontraste en
   Azure DevOps, e imprime un resumen en texto plano de los nodos y
   relaciones que quedaron.

No inventes nodos o relaciones que no puedas verificar con las tools —
si un repo no declara ninguna dependencia interna reconocible, no crees el
edge.
