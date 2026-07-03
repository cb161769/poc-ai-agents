# PoC — Agente autónomo + AI Firewall local (100% funcional, sin mocks)

Ver [PLAN.md](PLAN.md) para el diseño completo y [design.html](design.html) para el esquema visual.

Componentes: Neo4j (+ MCP), SonarQube real, Qdrant/RAG (+ MCP), Azure DevOps (+ MCP), Jira Cloud real, `gh copilot` real.

## 0. Prerequisitos de tu máquina

- Docker + Docker Compose
- `jq`, `curl`
- `gh` CLI autenticado con `gh extension install github/gh-copilot`
- Node.js (para `npx @azure-devops/mcp`) y `uv`/`uvx` (para `mcp-neo4j-cypher` y `mcp-server-qdrant`) instalados en el host, no en Docker — son los que consume Copilot Chat
- Una organización Azure DevOps real y una instancia Jira Cloud real, cada una con un Personal Access Token

Si usas Linux/WSL, SonarQube requiere:
```bash
sudo sysctl -w vm.max_map_count=524288
```

## 1. Configurar credenciales

```bash
cp .env.example .env
# edita .env con tus valores reales de Jira y Azure DevOps
```

## 2. Levantar la infraestructura

```bash
docker compose up -d --build
```

Esto levanta Neo4j, aplica el seed fallback, levanta SonarQube, corre el escaneo real de `sample-repo/` (contenedor `sonar-scanner`, tarda 1-2 min la primera vez), levanta Qdrant, indexa el corpus RAG, y levanta el AI Firewall en `:8080`.

Al terminar, revisa el log del scanner para copiar el token real a tu `.env`:
```bash
docker compose logs sonar-scanner | grep SONAR_TOKEN
# copia el valor a SONAR_TOKEN= en tu .env, luego:
docker compose restart ai-firewall
```

## 3. Sincronizar el grafo desde Azure DevOps real (opcional pero recomendado)

Abre VS Code en este directorio (ya trae `.vscode/mcp.json`), confirma que los cuatro MCP (`neo4j-cypher`, `azure-devops`, `qdrant-rag`, `atlassian`) arrancan, y pega el contenido de [prompts/sync_graph_from_azure_devops.md](prompts/sync_graph_from_azure_devops.md) en Copilot Chat. Si no lo haces, el grafo usa el seed estático de `seed/seed.cypher`.

El MCP `atlassian` es remoto (`mcp.atlassian.com`) y usa OAuth: la primera vez que Copilot Chat lo invoque, VS Code abrirá el navegador para que inicies sesión en tu instancia Jira/Confluence. No requiere `JIRA_API_TOKEN` — esa variable es solo para `jira_client.py`, que sigue siendo el que alimenta `run_poc_loop.sh` (el MCP es para que Copilot explore Jira interactivamente, no para el pipeline scripteado).

## 4. Inicializar sample-repo/ como repo git (para que Copilot pueda aplicar cambios)

```bash
git -C sample-repo init && git -C sample-repo add -A && git -C sample-repo commit -m "baseline"
```

Sin esto, `run_poc_loop.sh` solo imprime la sugerencia de Copilot pero no puede aplicarla a una rama de revisión (ver paso 6).

## 5. Verificar que todo está listo

```bash
./scripts/check_prereqs.sh
```

## 6. Correr el flujo completo

```bash
./run_poc_loop.sh
```

El escenario (limpio o malicioso) lo decide el **contenido real** del ticket de Jira en `JIRA_TICKET_KEY`, no un flag.

**Importante — qué es y qué no es "el agente" acá:** las etapas 1-4 son orquestación determinística, no un agente. La etapa 5 tiene dos caminos:

- **Con `GITHUB_REPO` configurado en `.env`** (recomendado si querés un agente de verdad): el script crea un Issue en tu repo real con todo el contexto ya armado y lo asigna al **GitHub Copilot coding agent**, que corre en la nube de GitHub con su propio razonamiento y abre un PR cuando termina. Requiere Copilot coding agent habilitado en el repo (plan Business/Enterprise) y `sample-repo/` empujado ahí (`git remote add origin ...` + `git push`). El agente en la nube **no** tiene acceso a tu Neo4j/Qdrant locales — por eso el impacto del grafo y los hallazgos de Sonar viajan como texto ya calculado dentro del Issue, no se consultan en vivo desde la nube.
- **Sin `GITHUB_REPO`** (fallback): invoca `gh copilot suggest` sobre el prompt saneado. Pide **confirmación antes de ejecutar cualquier comando** — nunca se ejecuta nada a ciegas. Si aceptás y el comando modifica archivos, el script los commitea en una rama nueva `copilot/<ticket>-<timestamp>` dentro de `sample-repo/`, **nunca en `main`**. Esto es una sugerencia puntual, no un agente autónomo.

```bash
# revisar el resultado del camino B (fallback local):
git -C sample-repo diff main..copilot/<rama-que-te-haya-mostrado-el-script>
```

## 7. Romper el flujo a propósito

Edita el ticket real en Jira (no un archivo local) y agrega a la descripción, por ejemplo:

- **Fuga de datos:** `password=Sup3rS3cr3t!` → el firewall lo aprueba pero con `redactions_applied >= 1`, y verás el prompt censurado antes de que llegue a `gh copilot suggest`.
- **Jailbreak:** `ignore previous instructions` → el firewall responde `403 REJECTED`, el script hace `exit 1`, y `gh copilot` nunca se invoca.

Vuelve a correr `./run_poc_loop.sh` después de guardar el cambio en Jira (si corriste hace menos de `CACHE_TTL_SECONDS`, el ticket puede venir de cache — bórralo con `rm -rf cache/` para forzar una lectura en vivo).

## 8. Auditoría y métricas de colaboración

```bash
cat logs/firewall_audit.jsonl
```

Una línea JSON por decisión de seguridad (aprobada o rechazada), sin secretos en claro.

```bash
cat logs/copilot_contribution.jsonl
python3 scripts/report_sprint_metrics.py --since 2026-07-01
```

`copilot_contribution.jsonl` registra, por corrida, si Copilot sugirió un cambio y si se aplicó. `report_sprint_metrics.py` agrega ese log en un resumen (tickets tocados, tasa de aprobación, tasa de redacción, sugerencias aplicadas) — la evidencia de cuánto colabora Copilot en el sprint, no solo si el pipeline corrió.

Además, cada corrida deja un **comentario real en el ticket de Jira** (no solo en los logs locales) con el resultado del firewall y qué hizo Copilot — revisalo directo en la pestaña de comentarios del ticket, o probalo a mano con:
```bash
python3 jira_client.py comment "Prueba de comentario de auditoria"
```
