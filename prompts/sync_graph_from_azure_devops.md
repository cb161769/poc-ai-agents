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
   `CREATE` — por cada nodo `:Service {name, language, buildTool, repo_url}`
   y cada relación `:DEPENDS_ON`, para que la operación sea idempotente si
   la vuelvo a correr después. `repo_url` es la URL real del repo en Azure
   DevOps (la misma tool de listado ya te la da) — sin este dato, el modo
   `--epic` de `run_poc_loop.sh`/`orchestration.py` no puede confirmar si
   dos componentes viven en el mismo repo, así que se niega a procesar
   épicas que los toquen a ambos.
4. Al final, confirma con `read_neo4j_cypher` (`MATCH (n) RETURN n.name,
   n.language, n.repo_url`) que el grafo resultante coincide con lo que
   encontraste en Azure DevOps, e imprime un resumen en texto plano de los
   nodos y relaciones que quedaron.

No inventes nodos o relaciones que no puedas verificar con las tools —
si un repo no declara ninguna dependencia interna reconocible, no crees el
edge.

5. Ownership (opcional): si el repo tiene un archivo `CODEOWNERS` (raíz o
   `.github/CODEOWNERS`), inspecciónalo y por cada regla que mapee el repo
   completo (o su carpeta raíz) a un equipo/usuario, aplica `MERGE (o:Owner
   {name: "<equipo o usuario>"})` y `MERGE (servicio)-[:OWNED_BY]->(o)`.
   Esto es curación de un dato real del repo, no algo que Azure DevOps
   entregue directamente — si el repo no tiene `CODEOWNERS`, omití este paso
   por completo en vez de inventar un owner. No confundas esto con quién
   está *asignado* a un ticket puntual: `OWNED_BY` es responsabilidad del
   componente en general, no de una tarea específica.
