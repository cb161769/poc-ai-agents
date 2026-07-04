# PoC — Agente autónomo + AI Firewall local (100% funcional, sin mocks)

Ver [PLAN.md](PLAN.md) para el diseño completo y [design.html](design.html) para el esquema visual.

Componentes: Neo4j (+ MCP), SonarQube real, Qdrant/RAG (+ MCP), Azure DevOps (+ MCP), Jira Cloud real, `gh copilot` real.

## 0. Prerequisitos de tu máquina

- Docker + Docker Compose
- `jq`, `curl`
- `gh` CLI autenticado con `gh extension install github/gh-copilot`
- Node.js (para `npx @azure-devops/mcp`) y `uv`/`uvx` (para `mcp-neo4j-cypher` y `mcp-server-qdrant`) instalados en el host, no en Docker — los consume tanto Copilot Chat como el agente juez (`judge_agent.py`), que corre en tu máquina y sí puede alcanzar tu Neo4j/Qdrant locales vía MCP (a diferencia del coding agent en la nube)
- Python 3.10+ en el host con las dependencias de `requirements.txt` instaladas (`pip install -r requirements.txt`) — `jira_client.py`, `sonar_client.py` y `judge_agent.py` corren directo en tu máquina, no dentro de Docker
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

**La detección del bug sigue siendo tuya**: nada acá monitorea microservicios en runtime. El script chequea si la descripción trae un bloque de código real (campo estructurado del ADF de Jira, no un regex adivinando palabras) — si pegaste el log como texto plano en vez de bloque de código, no cuenta como evidencia, y el script comenta en el ticket pidiéndolo bien formateado, mencionando el microservicio exacto — sin frenar la corrida.

Si el bug tiene un video/imagen adjunto y tu instancia tiene **Rovo** activo, el script lee la descripción que Rovo ya haya dejado como comentario (ajustá `ROVO_AUTHOR_NAME_MATCH` si tu instancia lo muestra con otro nombre) y la suma al prompt — sin descargar el video ni correr visión propia. Si el adjunto todavía no tiene descripción de Rovo, el script se detiene antes de llegar al firewall y pide revisión humana.

Apenas el firewall aprueba, el ticket se mueve automáticamente al estado que definas en `JIRA_IN_PROGRESS_STATUS` (default `"In Progress"`) — si tu workflow usa otro nombre, ajustalo en `.env`; si el nombre no matchea ninguna transición disponible, el script avisa pero sigue.

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

## 9. Testing agent (gate real de tests, antes del juez)

Solo en el Camino B con un cambio aplicado localmente: antes de llamar al juez, el script corre el **test suite real** del módulo afectado dentro de un contenedor descartable (`scripts/run_module_tests.sh`) — nada instalado en tu máquina, nada persistido:

- `AuthService`: JUnit 5 (`mvn test`).
- `Frontend`: Vitest (lógica) **+ Playwright** (`public/index.html` renderizado en un navegador headless real, con la llamada a `AuthService` interceptada — para probar UI real, no solo funciones en memoria).
- `DataWorker`: pytest.

Si los tests **fallan**, el pipeline se bloquea ahí mismo (comentario fuerte + transición a `JIRA_BLOCKED_STATUS` + commit `BLOCKED BY TESTS: ...` en la rama) y **el juez ni se llama**. Si pasan, el resultado se le pasa al juez como parte de su contexto — un test que pasa no prueba que su alcance sea suficiente para el cambio real, y el juez lo tiene en cuenta.

Requiere Docker en el host (ya es un prerequisito). Sin Docker, este gate se omite sin bloquear la corrida.

## 10. Monitoreo a nivel de sistema (Falco)

Además del firewall/juez (que auditan a nivel *semántico* — qué dice el ticket, qué hace el diff), `falco` monitorea en tiempo real qué hacen los contenedores a nivel de **sistema operativo** (syscalls): shells inesperados dentro de `poc-ai-firewall` o el testing agent, escrituras fuera de las rutas esperadas, conexiones salientes raras desde los contenedores efímeros de test. Reglas propias en [falco/custom_rules.yaml](falco/custom_rules.yaml).

```bash
docker logs -f poc-falco          # alertas en vivo
cat logs/falco_alerts.jsonl       # alertas persistidas
```

**En Windows/Docker Desktop**: Falco necesita que el kernel de la VM WSL2 soporte su probe moderno de eBPF (`--modern-bpf`) — si el contenedor no arranca o no genera eventos, es una limitación de correr Falco fuera de un host Linux nativo, no de esta PoC. Si te da problemas, podés comentar el servicio `falco` en `docker-compose.yml` sin afectar al resto del pipeline.

## 11. Agente juez (segunda opinión, con acceso real a MCP y poder de bloqueo)

Con `ANTHROPIC_API_KEY` configurada en `.env`, cada corrida aprobada por el firewall pasa además por un **modelo distinto** (Claude, no `gh copilot`) que audita: si el firewall decidió bien, si el cambio real resuelve el ticket, y si la corrida completa tiene sentido.

A diferencia del coding agent (que corre en la nube de GitHub y no puede tocar tu infraestructura local), **el juez corre en tu máquina** — así que se conecta de verdad a `mcp-neo4j-cypher` y `mcp-server-qdrant` por stdio, y puede *verificar* afirmaciones en vez de confiar ciegamente en el texto que le armamos (por ejemplo, consultar el grafo él mismo para confirmar si un cambio realmente no afecta a otros servicios). Si esos MCP no están disponibles (falta `uvx`, Neo4j/Qdrant caídos), el juez sigue funcionando sin herramientas, razonando solo sobre el texto.

**Backend del juez, sin depender de una API paga si no querés**: primero intenta Anthropic (`ANTHROPIC_API_KEY`); si no está configurada, cae al contenedor `ollama` local del `docker-compose.yml` (gratis, offline). Para que el fallback funcione, después de `docker compose up` descargá un modelo con tool-calling una sola vez:
```bash
docker exec poc-ollama ollama pull llama3.1
```
Si cambiás `OLLAMA_MODEL` en `.env`, descargá ese modelo en su lugar. Sin `ANTHROPIC_API_KEY` ni Ollama alcanzable, el juez se omite.

Si marca `FLAGGED`:

- Deja un comentario fuerte en el ticket de Jira.
- Mueve el ticket a `JIRA_BLOCKED_STATUS` (default `"Blocked"` — ajustalo a tu workflow).
- Si hubo un cambio aplicado en una rama local, la marca con un commit `BLOCKED BY JUDGE: ...` en su propio historial — no la mergees sin revisarla.
- Si fue al coding agent en la nube, intenta retirarle la asignación (mejor esfuerzo).

Sin `ANTHROPIC_API_KEY`, el juez se omite y la corrida sigue igual que antes. Cada veredicto queda en:
```bash
cat logs/judge_verdicts.jsonl
```
