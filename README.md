# PoC — Agente de código con supervisión humana + AI Firewall local

**Estado: prueba de concepto (PoC) funcional, sin mocks.** Todos los componentes hablan con servicios reales (Jira Cloud, SonarQube, Neo4j, Qdrant, Azure DevOps, GitHub Copilot / Anthropic / Ollama) — nada acá simula una respuesta. Eso significa que también hereda las limitaciones reales de esos servicios (ver [Limitaciones](#limitaciones-reales)). No es un producto terminado ni un framework reusable fuera de este repo.

## Qué es esto

Un ticket de Jira dispara un pipeline que: lee el ticket real, evalúa su contenido contra un **AI Firewall** (detecta fugas de datos y jailbreaks antes de que lleguen a cualquier modelo), consulta el **grafo de dependencias** en Neo4j y los **hallazgos de SonarQube** del componente afectado, arma un prompt con ese contexto, se lo pasa a un **agente de código** (GitHub Copilot coding agent en la nube, o un agente local propio con Claude/Ollama) que aplica el cambio en una rama real, corre el **test suite real** del proyecto afectado, y finalmente pasa por un **agente juez** independiente que audita si la decisión de seguridad y el cambio tienen sentido — con poder real de bloquear la corrida si algo no cierra.

El problema que resuelve: mostrar, con servicios reales y sin mocks, qué hace falta para que un agente de código autónomo sea *auditable* — quién decidió qué, con qué evidencia, y quién puede frenarlo — en vez de solo demostrar que "un LLM puede escribir código".

Ver [PLAN.md](PLAN.md) para el diseño completo, [design.html](design.html) para el esquema visual de arquitectura, y [pitch.html](pitch.html) para el pitch ejecutivo (problema, cadena de evidencia, decisiones de arquitectura, prueba real).

## Quick start

El camino más corto para ver el pipeline correr una vez, sin entrar todavía en todas las opciones:

```bash
# 1. Prerrequisitos: Docker, jq, curl, gh CLI (con gh-copilot), Python 3.10+

# 2. Un solo comando: crea .env si no existe, levanta Neo4j/SonarQube+scan
#    real/Qdrant/firewall/Ollama, aplica el SONAR_TOKEN real y espera el
#    modelo de Ollama -- ver scripts/setup.sh
./scripts/setup.sh
# editá .env con tus credenciales reales: como mínimo JIRA_URL, JIRA_EMAIL,
# JIRA_API_TOKEN, JIRA_TICKET_KEY

# 3. Pararte en el repo real al que corresponde el ticket, y correr
cd /ruta/a/tu-proyecto-real
/ruta/a/poc-ai-agents/run_poc_loop.sh   # o con un ticket puntual: run_poc_loop.sh JIRA-123
```

Esto alcanza para ver el flujo completo (firewall → grafo/Sonar → agente → tests → juez) con la infraestructura mínima. `scripts/setup.sh` es idempotente (correrlo de nuevo no rompe nada) y reemplaza los pasos manuales de copiar el `SONAR_TOKEN` a mano y bajar el modelo de Ollama a mano — si preferís hacerlo paso a paso (o `setup.sh` no puede confirmar algún paso), la [guía completa](#guía-completa-paso-a-paso) más abajo tiene el detalle manual de cada uno. También cubre cada pieza opcional: sync del grafo desde Azure DevOps, Figma, épicas, Falco, evals, Prefect.

## Arquitectura

```
Jira Cloud (ticket real)
       │  jira_client.py
       ▼
AI Firewall (:8080) ──rechaza──► REJECTED (403), corrida termina
       │  aprueba (con redacciones si hubo fuga de datos)
       ▼
Contexto compuesto: grafo Neo4j (impacto real) + hallazgos SonarQube (componente)
  [+ Figma specs si el ticket trae link]  [+ descripción de Rovo si hay adjunto]
  [+ orden real por dependencia + conflictos si es una épica -- epic_planner.py, §6.1]
       ▼
Agente de código (uno de tres caminos, ver §6):
  A) GitHub Copilot coding agent (nube, PR async) -- issue_body redactado antes de publicarse (§6.3)
  B1) coding_agent.py (agente local, Claude/Ollama, tool-calling con confirmación humana,
      autocrítica self_review antes de terminar -- §6.4)
  B2) gh copilot suggest (sugerencia de un tiro, fallback sin backend de modelo)
       ▼
output_guard.py — mismas reglas del firewall aplicadas al DIFF que sale (no solo al prompt de entrada)
       │  encuentra evidencia real ──► BLOCKED, corrida termina
       ▼
Testing agent (scripts/run_module_tests.sh) — test suite real del módulo + lint/format advisory,
  en contenedor descartable; rescan_sonar.sh re-escanea el diff real (§9.1)
       │  falla ──► BLOCKED, corrida termina
       ▼
Falco (opcional) — correlación de syscalls de ESTA MISMA ventana, se le pasa al juez antes de que decida
       ▼
Agente juez (judge_agent.py) — modelo distinto, con MCP real a Neo4j/Qdrant, ve self_review + Falco +
  conflictos de épica + hallazgos nuevos de Sonar, puede volver a bloquear o pedir un reintento acotado
       │  OK ──► push + PR real (Camino B1) + ticket a JIRA_REVIEW_STATUS (§6.5)
       ▼
Comentario + transición de estado en el ticket de Jira real, corrida completa como evidencia en el
  grafo de Neo4j (graph_writer.py, §8.1), logs de auditoría en logs/*.jsonl
```

Todo lo anterior corre por default vía `run_poc_loop.sh` (bash secuencial). `orchestration.py` es el mismo pipeline orquestado por **Prefect** (reintentos, UI de grafo, estado persistido) — ver §13.

## Estructura del repositorio

```
run_poc_loop.sh / orchestration.py   Punto de entrada del pipeline (bash vs. Prefect)
pipeline_shared.py                   Constantes/logica compartidas entre AMBOS orquestadores (fuente unica, ver §6.2)
jira_client.py                       Lee/comenta/transiciona tickets Jira reales
sonar_client.py                      Consulta hallazgos reales de SonarQube (con cache)
firewall_proxy.py                    AI Firewall: detección de fugas de datos y jailbreaks (entrada)
output_guard.py                      Guardia de salida: mismas reglas del firewall, aplicadas al diff/issue que SALE del agente (§6.3)
judge_agent.py                       Agente juez independiente (segunda opinión, MCP real)
epic_planner.py                      Planificador de épicas: orden real por dependencia + conflictos (§6.1)
graph_writer.py                      Escribe cada corrida como evidencia real en el grafo de Neo4j (§8.1)
agent_loop.py                        Maquinaria compartida de tool-calling (backend dual, MCP, prompt caching, thinking, structured outputs -- §11.1)
conversation_memory.py               Compacta conversaciones largas y resume el contexto de una épica antes de sumarlo al prompt (§6.1)
llm_backends.py                      Registro de backends LLM (orden de preferencia, pricing) -- ver §13.1
coding_agent.py                      Agente de código local (Camino B1)
pr_webhook.py                        Servicio FastAPI: dispara el pipeline real desde comentarios de PR de Azure DevOps (§6.7)
chat.py                              Chat interactivo con el mismo backend dual + todas las tools (§14)
figma_client.py                      Specs de Figma vía REST cuando el ticket trae un link
cache_utils.py                       Cache genérico con TTL usado por los clientes reales (CACHE_DIR: dejalo vacío, ver .env.example)

scripts/
  setup.sh                           Un solo comando: .env + infra + SONAR_TOKEN real + modelo Ollama
  check_prereqs.sh                   Valida que la infraestructura esté arriba (incluye si Copilot coding agent es asignable de verdad)
  health_check.sh --json             Monitoreo continuo (cron/systemd) de los mismos servicios, postea a ALERT_WEBHOOK_URL si algo cae
  smoke_test.sh                      Corre el pipeline real de punta a punta con datos descartables
  run_module_tests.sh                Testing agent: auto-detecta stack, corre el test suite real + lint/format advisory
  bootstrap_sonar.sh                 Rota el admin de Sonar, genera el token real, escanea sample-repo/ (corre en el contenedor sonar-scanner)
  rescan_sonar.sh                    Re-escanea el repo objetivo DESPUÉS del cambio del coding agent (§9.1)
  report_sprint_metrics.py           Agrega logs/copilot_contribution.jsonl en métricas de sprint
  review_judge_verdicts.py           Curación humana de veredictos del juez
  promote_reviews_to_evals.py        Promueve revisiones humanas a casos de eval
  check_falco_alerts.py              Correlaciona alertas de Falco con la ventana de cada corrida
  run_epic_dood.sh                   Corrida real de --epic vía Docker-outside-of-Docker (mounts, red de docker-compose, clonado autenticado -- §6.1.1)

evals/
  judge_eval_cases.jsonl             Dataset etiquetado a mano para benchmarking del juez (con expected_policy_reference)
  JUDGE_POLICY.md                    Rúbrica versionada de criterios de bloqueo (policy_reference)
  run_judge_evals.py                 Corre el dataset contra judge_agent.py real, mide accuracy de veredicto Y de policy_reference

falco/custom_rules.yaml              Reglas propias de monitoreo a nivel de sistema operativo
seed/seed.cypher                     Seed fallback del grafo si no se sincronizó desde Azure DevOps
sample-repo/                         Referencia de qué archivo de proyecto necesita cada stack
prompts/                             Prompts para pegar en Copilot Chat (sync de grafo, Figma)
tests/                               Tests unitarios de la herramienta misma (no del repo objetivo)
logs/                                Auditoría real por corrida (gitignored)
docker-compose.yml                   Neo4j, SonarQube, Qdrant, AI Firewall, Ollama, Falco, Prefect
Dockerfile.firewall                  Imagen del AI Firewall / rag-indexer (Python + deps del proyecto)
Dockerfile.testrunner                Imagen para correr tests/pipeline dentro de Docker sin depender del PATH del host (§0.1)
```

## Casos de uso

- **Ver el firewall en acción**: editar un ticket real con `password=Sup3rS3cr3t!` o `ignore previous instructions` en la descripción y correr el pipeline — ver §7.
- **Auditar qué tan bien colabora un agente de código**: `report_sprint_metrics.py` sobre `logs/copilot_contribution.jsonl` — tasa de aprobación, tasa de redacción, tests pasados.
- **Medir la precisión del juez contra un dataset propio**: `evals/run_judge_evals.py`, curado con `scripts/review_judge_verdicts.py` a partir de corridas reales — §12.
- **Correr todo con reintentos y una UI de grafo en vez de leer texto de terminal**: `orchestration.py` sobre Prefect — §13.
- **Procesar una épica completa en un solo prompt coordinado** en vez de historia por historia — §6.1.

## Guía completa paso a paso

### 0. Prerrequisitos de tu máquina

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

#### 0.1. Alternativa: correr tests/pipeline dentro de Docker (sin depender del PATH del host)

En Windows es común que `python3` resuelva al stub de Microsoft Store en vez del intérprete real, o que falten `jq`/`cypher-shell` en el `PATH` — en vez de pelearte con eso, `Dockerfile.testrunner` da un entorno con todo resuelto (Python real, `git`, `jq`, `cypher-shell`, dependencias de `requirements.txt`):

```bash
docker build -f Dockerfile.testrunner -t poc-ai-agents-testrunner .
docker run --rm -v "$(pwd):/repo" -w /repo poc-ai-agents-testrunner python3 -m pytest tests/ -v
```

Para correr el pipeline real (no solo los tests) conectado a los servicios de `docker-compose.yml`, agregá `--network poc-ai-agents_poc-net` y apuntá las variables `*_URL`/`NEO4J_URI` de tu `.env` a los nombres de servicio (`ai-firewall`, `neo4j`, `sonarqube`, `qdrant`, `ollama`, `prefect-server`) en vez de `localhost` — dentro del contenedor, `localhost` es el contenedor mismo, no tu host.

### 1. Configurar credenciales

```bash
cp .env.example .env
# edita .env con tus valores reales de Jira y Azure DevOps
```

**Sobre `repository_origen`** (qué componente afecta el ticket): el pipeline primero mira el campo nativo **Components** del ticket de Jira (Settings → Components de tu proyecto), con fallback a labels si no hay match ahí. El set de componentes válidos se deriva automáticamente de los nombres de nodo reales en Neo4j en cada corrida — `JIRA_KNOWN_COMPONENTS` en `.env` es solo el fallback para cuando Neo4j todavía no está levantado.

### 2. Levantar la infraestructura

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
(`./scripts/setup.sh` hace estos dos pasos automáticamente — ver Quick Start.)

Opcional pero recomendado: seteá `FIREWALL_API_KEY` en `.env` antes de levantar `ai-firewall` para que `/evaluate` exija el header `X-Firewall-Key` (401 sin él) — sin esto, cualquiera que llegue a `FIREWALL_URL` puede pegarle al firewall directo, sin pasar por Jira. `run_poc_loop.sh`/`orchestration.py` mandan el header automáticamente si la variable está seteada.

### 3. Sincronizar el grafo desde Azure DevOps real (opcional pero recomendado)

Abre VS Code en este directorio (ya trae `.vscode/mcp.json`), confirma que los cinco MCP (`neo4j-cypher`, `azure-devops`, `qdrant-rag`, `atlassian`, `figma-dev-mode`) arrancan, y pega el contenido de [prompts/sync_graph_from_azure_devops.md](prompts/sync_graph_from_azure_devops.md) en Copilot Chat. Si no lo haces, el grafo usa el seed estático de `seed/seed.cypher`.

Para construir/ajustar pantallas reales con specs de Figma (medidas, colores, tokens), abre **Figma Desktop** con el archivo de diseño, activa "Enable local MCP Server" en Preferencias, y pega [prompts/build_ui_from_figma.md](prompts/build_ui_from_figma.md) en Copilot Chat — no requiere token, el servidor local ya tiene el contexto del archivo abierto. Esta vía es **interactiva**: un humano decide qué frame mirar.

**Vía automatizada (sin Figma Desktop abierto)**: si la descripción del ticket de Jira trae un link real de Figma (`figma.com/file/...` o `figma.com/design/...?node-id=...`), y seteaste `FIGMA_API_TOKEN` en `.env` (Personal Access Token real, distinto del MCP), el pipeline (`run_poc_loop.sh`/`orchestration.py`) lo detecta solo y `figma_client.py` le pega a la REST API real de Figma para sumar esas specs (colores/medidas/texto del nodo indicado) al prompt compuesto — igual que ya hace con el grafo de Neo4j y los hallazgos de Sonar. Sin link en el ticket o sin la variable seteada, esta sección simplemente se omite, sin bloquear la corrida.

El MCP `atlassian` es remoto (`mcp.atlassian.com`) y usa OAuth: la primera vez que Copilot Chat lo invoque, VS Code abrirá el navegador para que inicies sesión en tu instancia Jira/Confluence. No requiere `JIRA_API_TOKEN` — esa variable es solo para `jira_client.py`, que sigue siendo el que alimenta `run_poc_loop.sh` (el MCP es para que Copilot explore Jira interactivamente, no para el pipeline scripteado).

### 4. Pararte en el repo real que corresponde al ticket

`run_poc_loop.sh`/`orchestration.py` ya **no** usan un repo de ejemplo: operan sobre el repositorio git en el que estás parado (`cd`) al invocarlos, detectado con `git rev-parse --show-toplevel`. Antes de correr el paso 6:

```bash
cd /ruta/a/tu-proyecto-real   # el repo al que corresponde el ticket de Jira
```

El pipeline se niega a arrancar si no estás dentro de un repo git, o si el working tree tiene cambios sin commitear (para no mezclar tu trabajo en progreso con lo que aplique Copilot) — hacé commit o `git stash` antes de correr.

`sample-repo/` sigue en este proyecto solo como referencia de qué archivo de proyecto necesita cada stack (`pom.xml`, `go.mod`, `package.json`, etc.) para que la auto-detección de `scripts/run_module_tests.sh` lo reconozca — no es el default de nada.

### 5. Verificar que todo está listo

```bash
./scripts/check_prereqs.sh
```

#### 5.1. Smoke test end-to-end (opcional, recomendado tras cualquier cambio)

`check_prereqs.sh` valida que los servicios estén arriba; esto va un paso más allá y corre el pipeline real de punta a punta, sin mocks:

```bash
./scripts/smoke_test.sh
```

Crea un **ticket Jira real y descartable** (etiquetado `smoke-test`, cerrado automáticamente al final), un **repo git temporal** limpio, y corre el `run_poc_loop.sh` real contra ambos — validando en serio la lectura de Jira, la consulta al grafo, los hallazgos de Sonar y la evaluación del firewall (con su comentario y transición reales en el ticket).

**Qué NO cubre, a propósito**: la etapa 6 (coding agent) queda afuera — `gh copilot suggest` es una TUI interactiva que no se puede automatizar sin volverse frágil o mockear (algo que este proyecto evita), y el coding agent en la nube necesita un repo GitHub real y abre el PR de forma asíncrona. El ticket sintético se cierra automáticamente al terminar; si querés validar también esa etapa, corré `./run_poc_loop.sh` a mano (sin `SMOKE_TEST_MODE`) con un ticket real propio.

### 6. Correr el flujo completo

```bash
./run_poc_loop.sh                # usa JIRA_TICKET_KEY de .env
./run_poc_loop.sh JIRA-123       # o cualquier ticket real, pasado como argumento
# equivalente con orchestration.py: python3 orchestration.py [JIRA-123]
```

El escenario (limpio o malicioso) lo decide el **contenido real** del ticket de Jira, no un flag — y no estás atado a un solo ticket fijo en `.env`: pasale cualquier ticket que le compartas a Copilot como primer argumento.

**La detección del bug sigue siendo tuya**: nada acá monitorea microservicios en runtime. El script chequea si la descripción trae un bloque de código real (campo estructurado del ADF de Jira, no un regex adivinando palabras) — si pegaste el log como texto plano en vez de bloque de código, no cuenta como evidencia, y el script comenta en el ticket pidiéndolo bien formateado, mencionando el microservicio exacto — sin frenar la corrida.

Si el bug tiene un video/imagen adjunto y tu instancia tiene **Rovo** activo, el script lee la descripción que Rovo ya haya dejado como comentario (ajustá `ROVO_AUTHOR_NAME_MATCH` si tu instancia lo muestra con otro nombre) y la suma al prompt — sin descargar el video ni correr visión propia. Si el adjunto todavía no tiene descripción de Rovo, el script se detiene antes de llegar al firewall y pide revisión humana.

Apenas el firewall aprueba, el ticket se mueve automáticamente al estado que definas en `JIRA_IN_PROGRESS_STATUS` (default `"In Progress"`) — si tu workflow usa otro nombre, ajustalo en `.env`; si el nombre no matchea ninguna transición disponible, el script avisa pero sigue.

**Importante — qué es y qué no es "el agente" acá:** las etapas 1-4 son orquestación determinística, no un agente. La etapa 5 (agente de código) tiene tres caminos posibles:

- **A — Con `GITHUB_REPO` configurado en `.env`**: el script crea un Issue en tu repo real con todo el contexto ya armado y lo asigna al **GitHub Copilot coding agent**, que corre en la nube de GitHub con su propio razonamiento y abre un PR cuando termina. Requiere Copilot coding agent habilitado en ese repo (plan Business/Enterprise) y que ya tenga un remote real (`git push`) — `check_prereqs.sh` y el propio pipeline verifican de antemano si el bot realmente aparece como asignable (§6.6), en vez de descubrirlo recién cuando la asignación falla. El agente en la nube **no** tiene acceso a tu Neo4j/Qdrant locales — por eso el impacto del grafo y los hallazgos de Sonar viajan como texto ya calculado dentro del Issue, no se consultan en vivo desde la nube. El `issue_body` se redacta con las mismas reglas del firewall antes de publicarse (§6.3) — puede terminar en un issue público de GitHub.
- **B1 — Sin `GITHUB_REPO`, con `ANTHROPIC_API_KEY` u Ollama alcanzable**: `coding_agent.py`, un **agente real local** — mismo backend dual (Anthropic primero, Ollama de fallback) y la misma maquinaria de tool-calling que ya usa el agente juez (`agent_loop.py`). Razona en varios turnos, con herramientas reales confinadas al repo objetivo: leer/escribir/editar/listar archivos, buscar texto (`grep`), ver su propio diff/historial de commits, detectar el stack del proyecto, consultar Sonar en vivo, correr comandos de shell, y los mismos MCP del juez (Neo4j-cypher, Qdrant-rag) para consultar el grafo/código histórico por su cuenta — incluyendo el historial real de riesgos ya documentados para el componente que está tocando. **Cada escritura de archivo y cada comando piden confirmación humana antes de ejecutarse** — se ven y se responden en vivo en la terminal (`[s/n]`), nunca actúa sin supervisión. Antes de declararse terminado, se autoevalúa (`self_review`, §6.4) y esa autocrítica viaja al juez para que la contraste contra el diff real. Si el juez aprueba, el cambio se pushea y se abre un PR real automáticamente (§6.5).
- **B2 — Sin `GITHUB_REPO` y sin ningún backend de modelo**: cae a `gh copilot suggest` sobre el prompt saneado — una sugerencia de un solo tiro, sin memoria ni loop, con la misma confirmación antes de ejecutar. Es el fallback de siempre, para quien no tiene `ANTHROPIC_API_KEY` ni Ollama corriendo.

En B1 y B2, el resultado (si hay cambios) se commitea en una rama nueva `copilot/<ticket>-<timestamp>` dentro del repo real detectado en el paso 4, **nunca en la rama que tenías checked out**.

```bash
# revisar el resultado del camino B (B1 o B2), desde el repo real:
git diff <rama-base-real>..copilot/<rama-que-te-haya-mostrado-el-script>
```

#### 6.1. Trabajar con épicas (con planificación real, no solo concatenación)

```bash
./run_poc_loop.sh --epic EPIC-123
# equivalente: python3 orchestration.py --epic EPIC-123
```

En vez de un ticket individual, trae la **épica completa + todas sus historias hijas** (vía JQL — por default `parent = "{epic_key}"`, el campo estándar en proyectos "team-managed"; si tu proyecto es "company-managed" y todavía usa el campo custom `Epic Link`, ajustá `JIRA_EPIC_LINK_JQL` en `.env`).

**`epic_planner.py`** entra antes de armar el prompt combinado: consulta el grafo real de Neo4j (`DEPENDS_ON` entre los componentes que tocan las historias hijas) y reordena las historias por dependencia real — no por el orden mecánico en que Jira devolvió el JQL. Si encuentra coordinación necesaria entre historias (ej. dos tocan el mismo endpoint), lo suma como notas de coordinación al prompt; si detecta **conflictos** reales entre historias, los deja explícitos tanto en el prompt del coding agent como en el payload que evalúa el juez — para que el propio juez pueda marcar `FLAGGED` si el diff final no muestra evidencia de haberlos tenido en cuenta. Es best-effort: si no hay backend de modelo disponible, cae sola al orden mecánico original sin bloquear la corrida.

**Solo funciona si todos los componentes que tocan los hijos de la épica viven en el mismo repo.** El pipeline está construido alrededor de un repo por corrida (`TARGET_REPO_DIR`) — si los hijos tocan componentes que en realidad viven en repos distintos, no hay forma honesta de resolverlo en una sola corrida, así que **se niega a trabajar** en vez de intentarlo a medias. Para poder confirmar esto, cada nodo del grafo de Neo4j necesita la propiedad `repo_url` seteada (ver `seed/seed.cypher` y `prompts/sync_graph_from_azure_devops.md`) — sin ese dato en cualquiera de los componentes involucrados, también rechaza la corrida (nunca asume que es el mismo repo sin poder confirmarlo). Cuando rechaza, deja un comentario claro en la épica explicando el conflicto (o el dato faltante) antes de terminar.

Si todo coincide, sigue el mismo flujo que un ticket normal (firewall → coding agent → output_guard → testing agent → Falco → juez) usando la épica como `TICKET_ID` (ramas/commits quedan como `copilot/EPIC-123-<timestamp>`), y al final deja un comentario en cada historia hija señalando que se procesó como parte de esa corrida combinada.

**`epic_planner.py` asume que las historias hijas ya existen -- no las crea.** Si la épica llega vacía (`children: []`, como pasó con `KAN-4`, que además resultó ser un meta-prompt de inyección de rol en vez de una épica real -- ver §11/firewall), el pipeline se niega a procesarla en vez de adivinar. El camino recomendado en ese caso es `prompts/decompose_epic_with_rovo.md`: un prompt para correr a mano en una sesión interactiva de Claude Code con el MCP `atlassian`/Rovo activo, que lee el contenido real de la épica (deteniéndose si detecta que el contenido es en sí mismo una instrucción para el modelo en vez de alcance de negocio), propone historias hijas candidatas (con confirmación humana explícita antes de crear nada) y las crea en Jira como hijas reales de la épica. No es parte del pipeline automatizado porque el MCP real de Atlassian es remoto con OAuth interactivo, y `orchestration.py`/`run_poc_loop.sh` corren headless -- Rovo resuelve la *creación* de historias, `epic_planner.py` sigue resolviendo el *orden* de ejecución una vez que existen.

Validado en vivo contra `KAN-4`: Rovo detectó el meta-prompt y no lo siguió, extrajo el alcance de negocio real embebido en el ticket (un listado concreto de páginas de un portal corporativo), propuso 5 historias candidatas, y tras confirmación explícita las creó como hijas reales (`KAN-11`..`KAN-15`). Un hallazgo real de esa prueba: el proyecto de Jira de esta PoC es "team-managed" y **no tiene el campo Components configurado en absoluto** (confirmado con `getJiraIssueTypeMetaWithFields` -- ni siquiera aparece como campo disponible) -- en ese caso, la única vía real para que `_resolve_repository_origen()` reconozca las historias nuevas es el fallback de **labels**, asignando manualmente un label que matchee un nombre de `JIRA_KNOWN_COMPONENTS` (ej. `Frontend`). Si tu proyecto sí usa Components, preferí ese campo en su lugar.

**Limitación real, a propósito no resuelta todavía**: el reordenamiento de `epic_planner.py` es hoy solo textual — cambia el orden en que el coding agent *lee* las historias en el prompt, pero el coding agent sigue recibiendo un único prompt combinado y decide su propio orden de ejecución interno. No hay enforcement real de secuencia (ej. aplicar la historia A, commitear, recién después empezar la B). Resolverlo bien implicaría partir el modo épica en llamadas secuenciales al coding agent, un cambio de arquitectura más grande que no se hizo todavía.

**`EPIC_PLANNER_OLLAMA_MODEL` importa de verdad**: sin backend de Anthropic, `plan_epic()` cae al Ollama local -- si no seteás este override, usa el `OLLAMA_MODEL` genérico (default `llama3.1`), que en la práctica alucina seguido en esta tarea (ordenar JSON de tickets) y degrada silenciosamente al orden mecánico de Jira, sin ningún error visible. Gap real confirmado: una épica intentó aplicar una historia que agregaba un componente a un framework *antes* que la historia que montaba ese framework, porque el reordenamiento real nunca llegó a correr con éxito. Setealo igual que `CODING_AGENT_OLLAMA_MODEL`/`JUDGE_OLLAMA_MODEL` (§11.1).

**El modo épica corta la corrida COMPLETA ante la primera falla real de una historia hija** -- no es un bug, es la decisión de diseño explícita de `_deliver_epic_sequential()` (`orchestration.py`): si los tests reales fallan, o el juez marca `FLAGGED`, la épica entera se detiene ahí (comentario + transición a `JIRA_BLOCKED_STATUS` en esa historia, las hermanas restantes quedan sin tocar) en vez de seguir aplicando más historias sobre una rama compartida que ya se sabe rota. Es más parecido a un gate estricto de CI que a un sprint real (donde una historia que falla no bloquea el trabajo de las demás) -- ver [PLAN.md](PLAN.md) si necesitás procesar historias independientes entre sí sin este acoplamiento.

##### 6.1.1. Corriendo `--epic` real vía Docker-outside-of-Docker

`scripts/run_epic_dood.sh <EPIC_KEY> [TARGET_REPO_GIT_URL]` encapsula una corrida real de `--epic` dentro de `Dockerfile.testrunner` (con `/var/run/docker.sock` montado, para que el coding agent pueda delegar tests a imágenes reales por stack -- §9): clona/actualiza un clon persistente (`TARGET_REPO_CLONE_DIR`, default `.dood-target-repo/`) con la credencial pasada vía `http.extraheader` (nunca embebida en la URL -- eso `git` la persiste en texto plano en `.git/config`), traduce paths para el daemon del HOST en Windows (`cygpath`), y corre `orchestration.py --epic` con las URLs de servicio reescritas a los nombres de contenedor del `docker-compose.yml` (`poc-neo4j`, `poc-ollama`, etc. -- dentro de ese contenedor, `localhost` es el contenedor mismo, no `docker-compose`).

**Dos gotchas reales de Windows que este script resuelve, y que rompen si los tocás a mano**:
- **`core.autocrlf`**: el config **SYSTEM** de Git para Windows (invisible a `git config --global`/`--local`, solo aparece con `--show-origin`) suele traer `core.autocrlf=true` -- el host escribe CRLF al hacer checkout, pero los blobs del repo guardan LF. El host ve el árbol limpio (su propio git también espera CRLF ahí), pero un contenedor Linux comparando los mismos bytes contra los blobs reales encuentra cada línea como "modificada" -- un falso "árbol sucio" real que bloqueaba el pipeline antes de tocar una sola historia. El script persiste `core.autocrlf=false` en el config **local** del clon (no alcanza con `-c core.autocrlf=false` en el `clone` inicial -- esa bandera es transitoria, no sobrevive a un `checkout`/`reset --hard` posterior).
- **Identidad de git para el primer commit**: `git config --get user.name` (sin `--local`) también resuelve contra el config global del HOST -- si tu máquina ya tiene una identidad real seteada ahí, el chequeo de "¿hace falta configurar identidad?" da un falso positivo (encuentra *algo*, aunque el contenedor nunca vea ese config global) y nunca escribe nada en el config local del clon. El contenedor, sin `~/.gitconfig` montado, falla el primer `git commit` real con "Author identity unknown" -- descartando un cambio real que el agente sí había escrito. El script fuerza `--local` en ese chequeo.

#### 6.2. `pipeline_shared.py` — una sola fuente de verdad entre los dos orquestadores

`run_poc_loop.sh` (bash) y `orchestration.py` (Prefect) implementan el mismo pipeline dos veces, en dos lenguajes — un caso real de esto se desincronizó silenciosamente: `RETRYABLE_POLICY_REFERENCES` (qué categorías de veredicto `FLAGGED` del juez ameritan un reintento automático del coding agent) vivía definida tres veces — en `judge_agent.py`, duplicada a mano en `orchestration.py`, y duplicada a mano en un array bash en `run_poc_loop.sh`. La copia de Python tenía un test que la comparaba contra la original; la de bash no tenía ninguno, y nadie lo notó hasta que se auditó explícitamente. `pipeline_shared.py` es la fuente única ahora: `judge_agent.py`/`orchestration.py` la importan directo, `run_poc_loop.sh` la lee vía `python3 pipeline_shared.py retryable-policy-references` en vez de mantener su propia copia.

#### 6.3. Guardia de salida (`output_guard.py`) — el firewall también audita lo que SALE

El AI Firewall (`firewall_proxy.py`) audita el prompt que **entra** al coding agent, pero hasta hace poco nada auditaba el diff que **sale** de él — si el agente escribía un secreto real al "arreglar" algo, o un patrón de jailbreak terminaba en un comentario de código, nada lo atrapaba hasta Sonar (si cubría ese patrón) o el juez (si lo notaba). `output_guard.py` corre las **mismas reglas** (`firewall/policies.yaml`, vía `firewall_proxy._redact()`/`_check_jailbreak()` reusado directo, no reimplementado) contra el diff real del Camino B1, **antes** del testing agent — si encuentra evidencia dura, bloquea con la misma severidad que un test fallido (comentario fuerte, transición a bloqueado, commit marcador), y el juez ni se llama.

Para el Camino A (nube, sin diff local todavía), se aplica una versión acotada: el `issue_body` que se publica en GitHub pasa por la misma función de redacción de secretos (no el chequeo de jailbreak, que no aplica a contenido para humanos) antes de crearse — puede terminar en un issue público, así que vale la pena la misma barrida.

#### 6.4. Autocrítica del coding agent (`self_review`) — informa al juez, no decide por sí sola

Antes de declararse `"done"`, `coding_agent.py` tiene que completar una autoevaluación estructurada: `scope_matches_ticket`, `no_secrets_introduced`, `tests_adequate` (booleanos). Si falta, recibe un empujón único pidiéndosela; si tampoco la completa la segunda vez, se acepta igual (queda trazado como faltante, nunca bloquea infinito). Esa autocrítica viaja al payload del juez — que la contrasta explícitamente contra el diff real ("si dice que no introdujo secretos pero ves uno, señalalo") en vez de tomarla como verdad. **Deliberadamente no decide nada por sí sola**: una autoevaluación puede ser incorrecta, así que no dispara un retry automático ni un bloqueo — es señal para el juez, que es quien tiene la autoridad real de decisión.

#### 6.5. Push + PR automático y transición a revisión (solo si el juez dice OK)

Hasta hace poco, el Camino B1 nunca pasaba de un commit local — ni un PR, ni ninguna señal en Jira de que el trabajo estaba listo, aunque el juez lo hubiera aprobado. Ahora, si el veredicto final es `OK` y hay una rama con cambios reales: se intenta `git push` + `gh pr create` (best-effort — si el repo objetivo no tiene un remote real configurado, se omite sin bloquear, la rama queda local para pushear a mano), y el ticket se transiciona a `JIRA_REVIEW_STATUS` (default `"Code Review"`, ajustable en `.env`) — el mismo tipo de señal de "listo para revisión humana" que antes solo existía para el caso de bloqueo (`JIRA_BLOCKED_STATUS`).

#### 6.6. Detección proactiva de si Copilot coding agent está habilitado de verdad

Antes, la única forma de saber si el Copilot coding agent (Camino A) estaba realmente habilitado en `GITHUB_REPO` era crear un Issue y ver si la asignación fallaba — puro prueba y error. Ahora, tanto `check_prereqs.sh` como el propio pipeline consultan `Repository.suggestedActors` (GraphQL real de GitHub, capability `CAN_BE_ASSIGNED`) **antes** de crear el Issue — si el bot no aparece como asignable, avisa con un diagnóstico claro (revisar Settings → Copilot → Coding agent, plan Business/Enterprise) pero igual intenta crear+asignar (el chequeo puede tener falsos negativos por permisos del token, así que nunca bloquea el intento real).

#### 6.7. `pr_webhook.py` — disparar el pipeline real desde un comentario de PR

Servicio FastAPI standalone (`Dockerfile.webhook`, servicio `pr-webhook` del `docker-compose.yml`, puerto `:8090`) que escucha webhooks reales de Azure DevOps (`/webhooks/azure-devops`, protegido con `X-API-Key` si `PR_WEBHOOK_API_KEY` está seteada) sobre comentarios de Pull Request — permite pedirle una nueva corrida al pipeline sin volver a la terminal, comentando directo en la PR real. Detecta si el ticket referenciado es una Épica (`jira_client.fetch_ticket_live`, best-effort — ante cualquier falla, degrada al comportamiento de ticket individual en vez de bloquear la respuesta del webhook) y arma el comando `docker run` correspondiente (`--epic <KEY>` o `<KEY>` a secas) vía Docker-outside-of-Docker, igual que `scripts/run_epic_dood.sh`. El proceso disparado corre en un hilo demonio separado (`subprocess.Popen` + `.wait()` en background) para que el webhook responda `200` de inmediato sin bloquear, y sin dejar procesos zombie acumulándose en un servicio de larga duración.

### 7. Romper el flujo a propósito

Edita el ticket real en Jira (no un archivo local) y agrega a la descripción, por ejemplo:

- **Fuga de datos:** `password=Sup3rS3cr3t!` → el firewall lo aprueba pero con `redactions_applied >= 1`, y verás el prompt censurado antes de que llegue a `gh copilot suggest`.
- **Jailbreak:** `ignore previous instructions` → el firewall responde `403 REJECTED`; el juez audita ese rechazo (§11) pero nunca lo revierte, el script hace `exit 1`, y `gh copilot` nunca se invoca.

Vuelve a correr `./run_poc_loop.sh` después de guardar el cambio en Jira (si corriste hace menos de `CACHE_TTL_SECONDS`, el ticket puede venir de cache — bórralo con `rm -rf cache/` para forzar una lectura en vivo).

#### 7.1. Tests del código propio (no del repo objetivo)

`scripts/run_module_tests.sh` (§9) testea el repo *objetivo* del usuario. Esto es distinto: tests unitarios de la herramienta misma (`firewall_proxy.py`, `jira_client.py`, `sonar_client.py`, `cache_utils.py`, `judge_agent.py`, `coding_agent.py`, `agent_loop.py`, `orchestration.py`) — funciones puras y deterministas, y las tools de los agentes contra repos git reales en `tmp_path`, sin pegarle a Jira/Sonar/Neo4j reales (se mockea con `unittest.mock` solo donde hace falta).

```bash
pip install -r requirements.txt
pytest tests/ -v
```

### 8. Auditoría y métricas de colaboración

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

#### 8.1. Grafo de conocimiento: cada corrida queda como evidencia real en Neo4j

Más allá del grafo de dependencias estático (`:Service` + `DEPENDS_ON`, sembrado desde Azure DevOps o `seed/seed.cypher`), `graph_writer.py` extiende ese mismo grafo con evidencia real de ejecución: un nodo `:Run` por corrida, un nodo `:Story`/`:Epic` para el ticket, un nodo `:Decision` por cada etapa (firewall/output_guard/tests/juez) con su resultado, y — si el juez citó un `policy_reference` real de `evals/JUDGE_POLICY.md` — un nodo `:Risk` que se acumula por componente a través de corridas. Deliberadamente **no** guarda contenido sensible (diffs completos, descripciones crudas) — eso ya vive en `logs/*.jsonl`; el grafo solo guarda campos cortos, redactados con la misma `firewall_proxy._redact()`.

El valor real: un componente que ya tuvo un `data-leak-evidence` hace dos sprints queda consultable — `coding_agent.py`/`judge_agent.py` pueden preguntarle al grafo `MATCH (svc:Service {name: "X"})<-[:AFFECTS]-(r:Risk) RETURN r` antes de actuar sobre ese componente, en vez de partir de cero cada vez. Es una sugerencia en el prompt, no forzada — cada corrida trae `consulted_risk_graph` (true/false) en el resultado para poder auditar si realmente se está usando.

```bash
docker exec -it poc-neo4j cypher-shell -u neo4j -p test_password_local \
  "MATCH (r:Risk)-[:AFFECTS]->(s:Service) RETURN s.name, r.policy_reference, count(*) ORDER BY count(*) DESC"
```

Best-effort SIEMPRE, igual que el resto de las integraciones opcionales: si Neo4j no está alcanzable, se loggea y la corrida sigue sin escribir en el grafo.

### 9. Testing agent (gate real de tests, antes del juez)

Solo en el Camino B con un cambio aplicado localmente: antes de llamar al juez, el script corre el **test suite real** del módulo afectado dentro de un contenedor descartable (`scripts/run_module_tests.sh`) — nada instalado en tu máquina, nada persistido.

**Generaliza a cualquier lenguaje/framework por auto-detección**, no por una lista fija de componentes: `run_module_tests.sh` recibe directo la ruta del repo real detectado en el paso 4, mira qué archivo de proyecto hay en su raíz y elige imagen + comando solo — `pom.xml` → Maven, `*.csproj` → .NET, `go.mod` → Go, `Gemfile` → Ruby, `Cargo.toml` → Rust, `Pipfile`/`requirements.txt` → Python, y **`package.json` → cualquier stack de Node/TS** (NestJS, Angular, Ionic, Expo, React Native, Vitest/Jest plano), todos en la misma rama sin bifurcar por framework.

`sample-repo/` (JUnit 5 en `auth-service/`, Vitest + Playwright en `frontend/`, pytest en `data-worker/`) queda en el proyecto como ejemplo de referencia de esos archivos de proyecto — no es lo que el pipeline testea por defecto.

Si los tests **fallan**, el pipeline se bloquea ahí mismo (comentario fuerte + transición a `JIRA_BLOCKED_STATUS` + commit `BLOCKED BY TESTS: ...` en la rama) y **el juez ni se llama**. Si pasan, el resultado se le pasa al juez como parte de su contexto — un test que pasa no prueba que su alcance sea suficiente para el cambio real, y el juez lo tiene en cuenta.

Requiere Docker en el host (ya es un prerequisito). Sin Docker, este gate se omite sin bloquear la corrida — `orchestration.py`/`run_poc_loop.sh` lo detectan (`shutil.which("docker")`/`command -v docker`) y lo dicen explícitamente en vez de fingir que corrió. **Fuera de alcance**: UI real de apps móviles (Expo/React Native/Ionic) sobre emulador/dispositivo (Detox/Maestro/Espresso) — necesita virtualización de hardware, delicada dentro de Docker Desktop; esta auto-detección solo corre los tests unitarios/lógica de esos proyectos.

**Lint/format, advisory, no bloqueante**: en el mismo contenedor descartable, `run_module_tests.sh` corre además un chequeo de lint/format cuando detecta que el repo objetivo lo tiene configurado (`go vet`/`cargo clippy` siempre, por ser parte del toolchain; `eslint`/`rubocop`/`ruff`/`dotnet format`/checkstyle solo si encuentra el archivo de config correspondiente — nunca asume que existe). A propósito **no** es un gate duro como los tests: un repo objetivo arbitrario puede tener deuda de lint preexistente que no tiene nada que ver con este cambio puntual, así que el resultado se suma como contexto extra para el juez, no como un bloqueo automático.

#### 9.1. Re-escaneo real de Sonar sobre el diff aplicado

`sonar_client.py` (usado como contexto al principio de la corrida) solo **lee** análisis que ya existían — nada volvía a escanear después de que el coding agent aplicara su cambio, así que un hallazgo nuevo que el propio agente introdujera era invisible para el pipeline. `scripts/rescan_sonar.sh` corre justo después de que los tests pasan: dispara un `sonar-scanner` real sobre el repo objetivo (best-effort — si no tiene `sonar-project.properties` configurado, o `SONAR_TOKEN` no está seteado, se omite sin bloquear), compara los hallazgos de antes vs. después, y solo los **nuevos** (los que el diff realmente introdujo, no deuda técnica preexistente) se le pasan al juez como evidencia.

### 10. Monitoreo a nivel de sistema (Falco) — llega al juez de la MISMA corrida, no solo a un comentario después

Además del firewall/juez (que auditan a nivel *semántico* — qué dice el ticket, qué hace el diff), `falco` monitorea en tiempo real qué hacen los contenedores a nivel de **sistema operativo** (syscalls): shells inesperados dentro de `poc-ai-firewall` o el testing agent, escrituras fuera de las rutas esperadas, conexiones salientes raras desde los contenedores efímeros de test. Reglas propias en [falco/custom_rules.yaml](falco/custom_rules.yaml).

```bash
docker logs -f poc-falco          # alertas en vivo
cat logs/falco_alerts.jsonl       # alertas persistidas
```

**En Windows/Docker Desktop**: la imagen de Falco puede necesitar ajustar el driver según la versión (`-o engine.kind=modern_ebpf` en el `command:` del `docker-compose.yml`, la sintaxis de CLI cambia entre versiones de la imagen) y que el kernel de la VM WSL2 soporte su probe eBPF — algunos tracepoints puntuales pueden fallar al engancharse sin que eso tumbe la detección en general. Si el contenedor no arranca en absoluto, es una limitación de correr Falco fuera de un host Linux nativo, no de esta PoC; podés comentar el servicio `falco` en `docker-compose.yml` sin afectar al resto del pipeline.

**Las alertas se correlacionan ANTES de que el juez decida, no solo después en un comentario**: `run_judge()` (tanto en `run_poc_loop.sh` como en `orchestration.py`) busca la ventana de Falco de la corrida actual **antes** de invocar al juez, y si hay alertas reales, se las suma al payload que el juez evalúa — para que la evidencia de runtime de la MISMA corrida pueda pesar en el veredicto, no solo llegar como un comentario de Jira que un humano lee después de que la decisión ya se tomó. El mismo fetch también dispara el comentario/webhook de siempre. Es informativo para el juez, no un gate duro por sí solo — el juez decide qué peso darle.

#### 10.1. Riesgos aceptados de infraestructura

Decisiones de diseño con un trade-off real de seguridad/operación, tomadas a propósito para esta PoC — documentadas explícitamente en vez de asumidas silenciosamente:

- **`/var/run/docker.sock` montado en `falco`, `pr-webhook`, y el testrunner CLI (Docker-outside-of-Docker)**: cualquiera de esos contenedores tiene control efectivo sobre el daemon Docker del **host** (no solo sobre sí mismo) — es lo que le permite a Falco monitorear contenedores hermanos, y al pipeline lanzar tests reales/corridas anidadas. Es necesario para la arquitectura elegida (DooD), no un descuido; en un despliegue real de producción esto se resolvería con un daemon Docker dedicado y aislado, no el del host.
- **`.env` en texto plano en disco**: todas las credenciales (Jira, Azure DevOps, Anthropic, Sonar) viven en un archivo sin cifrar — sin vault ni secret manager. Aceptable para desarrollo local/PoC, no para un despliegue compartido.
- **Falco corre `privileged: true` con `pid: host`**: requerido para que pueda observar syscalls del sistema real, no solo de su propio namespace — reducir el privilegio le quitaría la función.
- **Credenciales sin monitoreo continuo por defecto**: `check_prereqs.sh` sólo valida Jira/Azure DevOps al momento de invocarse; `scripts/health_check.sh` sí las revisa (§ arriba, agregado tras confirmar en vivo que un token puede vencerse a mitad de una sesión de trabajo sin que nada lo note) pero necesita estar programado en cron/systemd para dar cobertura continua real — no corre solo.

### 11. Agente juez (segunda opinión, con acceso real a MCP y poder de bloqueo)

Cada corrida — **aprobada o rechazada por el firewall** — pasa además por un **modelo distinto** (Claude, no `gh copilot`) que audita: si el firewall decidió bien, si el cambio real resuelve el ticket, y si la corrida completa tiene sentido. Además del texto del ticket/diff, el juez recibe como contexto real: la autocrítica `self_review` del coding agent (§6.4), las alertas de Falco de esta misma corrida (§10), los hallazgos nuevos de Sonar sobre el diff (§9.1), y los conflictos que `epic_planner.py` haya detectado si es una épica (§6.1) — todo señal, ninguno decide por sí solo, el juez es quien pesa todo eso.

A diferencia del coding agent (que corre en la nube de GitHub y no puede tocar tu infraestructura local), **el juez corre en tu máquina** — así que se conecta de verdad a `mcp-neo4j-cypher` y `mcp-server-qdrant` por stdio, y puede *verificar* afirmaciones en vez de confiar ciegamente en el texto que le armamos (por ejemplo, consultar el grafo él mismo para confirmar si un cambio realmente no afecta a otros servicios, o volver a consultar Sonar en vivo). Si esos MCP no están disponibles (falta `uvx`, Neo4j/Qdrant caídos), el juez sigue funcionando sin herramientas, razonando solo sobre el texto. Cada veredicto trae `consulted_risk_graph` (si efectivamente llegó a consultar el historial de `:Risk` del componente en Neo4j, §8.1) — visibilidad de si la sugerencia de consultarlo se está siguiendo de verdad, sin forzarlo.

**Backend del juez, sin depender de una API paga si no querés**: primero intenta Anthropic (`ANTHROPIC_API_KEY`); si no está configurada, cae al contenedor `ollama` local del `docker-compose.yml` (gratis, offline) — este fallback corre de verdad tanto desde `run_poc_loop.sh` como desde `orchestration.py`. Para que funcione, después de `docker compose up` descargá un modelo con tool-calling una sola vez:
```bash
docker exec poc-ollama ollama pull llama3.1
```
Si cambiás `OLLAMA_MODEL` en `.env`, descargá ese modelo en su lugar. (`./scripts/setup.sh`/el servicio `ollama-pull` del `docker-compose.yml` hacen esto automáticamente.) Sin `ANTHROPIC_API_KEY` ni Ollama alcanzable (o ante cualquier falla de red al llamarlos), el juez se omite y la corrida sigue sin veredicto — nunca frena el pipeline por su ausencia.

#### 11.1. Corriendo solo con Ollama (sin Anthropic) — qué mejora la confiabilidad real

Confirmado en vivo esta sesión, con Anthropic sin crédito: `llama3.1` genérico casi nunca completa el loop de investigación con tools (ni el coding agent ni el juez). Tres mejoras reales, todas en `agent_loop.py`, compartidas por `coding_agent.py`/`judge_agent.py`/`epic_planner.py`:

- **`CODING_AGENT_OLLAMA_MODEL`/`JUDGE_OLLAMA_MODEL`/`EPIC_PLANNER_OLLAMA_MODEL` aceptan una lista coma-separada de candidatos** (orden = prioridad, ej. `qwen3:8b,qwen2.5-coder:7b`) — cuando el modelo activo alucina (JSON inválido incluso tras su reintento de corrección, o reporta `blocked` citando su propia confusión de formato en vez de una razón real sobre el ticket), cada agente cambia UNA vez por corrida al siguiente candidato realmente descargado (`ollama pull`), en vez de rendirse directo. `qwen3` (`ollama pull qwen3:8b`) es el modelo que los propios docs de Ollama usan como ejemplo principal de tool-calling y de razonamiento -- mejor punto de partida que un modelo genérico o uno afinado solo para autocompletar código.
- **Modo "thinking" real** (`OLLAMA_THINKING_ENABLED`, default `true`): para modelos que lo soportan (detectados por nombre — `qwen3`, `gpt-oss`, `deepseek-r1`, `deepseek-v3`), le da al modelo un espacio de razonamiento explícito (campo `thinking` de la respuesta, separado de `content`/`tool_calls` — no rompe nada del parseo existente) antes de decidir si llama una tool o responde ya. Confirmado en vivo: con esto, `qwen3:8b` pasó de nunca completar una investigación real a escribir y commitear cambios reales por primera vez en esta sesión.
- **JSON restringido por esquema real, no solo "algo de JSON"**: el reintento de corrección (`_final_text_with_json_retry`) usaba `format: "json"` de Ollama — que solo garantiza JSON sintácticamente válido, no el esquema que cada agente realmente espera. Ahora, cuando el caller conoce su esquema exacto (`CODING_AGENT_RESULT_SCHEMA`/`JUDGE_RESULT_SCHEMA`), se lo pasa a Ollama vía `format` como JSON Schema real (structured outputs) — decodificación restringida al esquema exacto, no solo a "es JSON".

Requiere al menos un modelo con tool-calling real descargado (`ollama pull qwen3:8b`) — `llama3.1` sigue siendo el default si no cambiás nada, pero no es el que se recomienda para investigar código sin Anthropic.

**Sobre una corrida `APPROVED`**, si marca `FLAGGED` tiene poder real de bloqueo:
- Deja un comentario fuerte en el ticket de Jira.
- Mueve el ticket a `JIRA_BLOCKED_STATUS` (default `"Blocked"` — ajustalo a tu workflow).
- Si hubo un cambio aplicado en una rama local, la marca con un commit `BLOCKED BY JUDGE: ...` en su propio historial — no la mergees sin revisarla.
- Si fue al coding agent en la nube, intenta retirarle la asignación (mejor esfuerzo).

**Sobre una corrida `REJECTED`**, el juez audita la decisión del firewall pero **nunca la revierte** — el firewall sigue siendo la última palabra en seguridad. Si el juez sospecha que fue un falso positivo (rechazó algo legítimo), deja un comentario de alerta pidiendo revisión humana, pero la solicitud sigue rechazada y `gh copilot`/el coding agent nunca se invocan igual.

Cada veredicto queda en:
```bash
cat logs/judge_verdicts.jsonl
```
Cada entrada trae `backend`, `latency_seconds`, `input_tokens`/`output_tokens` y `estimated_cost_usd` — ver §12.

**Modo reference-grounded (implementado, sin dato real en el pipeline hoy)**: `judge_agent.py` acepta un campo opcional `reference_answer` en el payload — si viene seteado, el prompt le pide al juez comparar el cambio explícitamente contra esa respuesta de referencia ("gold standard") en vez de evaluarlo solo en abstracto (modo `pointwise`, el que se usa siempre hoy). Está probado (`tests/test_judge_agent.py`) pero **ningún caller real lo setea todavía**: se revisó `jira_client.py` y no trae ningún campo de criterio de aceptación estructurado por ticket, que es el dato que haría falta para poblarlo de verdad. Queda disponible para el día que Jira (o el sistema de tickets que uses) traiga ese campo — conectarlo es agregar `reference_answer` al payload que arman `run_poc_loop.sh`/`orchestration.py` antes de invocar al juez.

### 12. Evals: benchmarking del juez y precisión del coding agent

**Precisión del agente juez** — un dataset fijo de casos etiquetados a mano (`evals/judge_eval_cases.jsonl`: tickets + decisión de firewall + diff, cada uno con el veredicto que *debería* dar un humano):
```bash
python3 evals/run_judge_evals.py
```
Corre cada caso contra `judge_agent.judge_with_tools()` de verdad (mismo código que usa `run_poc_loop.sh`/`orchestration.py`), imprime una matriz de confusión (tratando `FLAGGED` como la clase positiva — el error grave es un falso negativo: algo que debía bloquearse y el juez dejó pasar) y el costo/latencia total. Falla (`exit 1`) si hay algún falso negativo. **Además** mide, por separado, si el juez citó el `policy_reference` correcto de `evals/JUDGE_POLICY.md` en cada caso `FLAGGED` — un veredicto puede ser correcto (bloqueó lo que había que bloquear) mientras cita el criterio equivocado, y esa métrica separada lo expone en vez de esconderlo detrás de un accuracy agregado. Resultados acumulados en `logs/eval_judge_runs.jsonl`.

**Precisión del coding agent (proxy)** — no hay forma automática de saber "¿resolvió el ticket correctamente?" sin un humano, pero sí hay un piso medible: ¿pasó los tests reales que se supone que tiene que pasar? Eso ya lo agrega `report_sprint_metrics.py` (§8), leyendo el campo `tests_passed` que ahora graba `run_poc_loop.sh` en `copilot_contribution.jsonl` cada vez que el testing agent corre.

#### 12.1. Mejora continua del juez (curación humana de evals, no reentrenamiento)

`evals/judge_eval_cases.jsonl` empezó como 7 casos fijos escritos a mano — esto lo hace crecer con corridas reales, sin tocar pesos de ningún modelo (nada acá hace fine-tuning; es curación de un dataset por un humano):

```bash
python3 scripts/review_judge_verdicts.py    # 1. revisa corridas reales pendientes, vos confirmas o corregís el veredicto del juez
python3 scripts/promote_reviews_to_evals.py # 2. las revisiones (acuerdos Y correcciones) se suman como casos nuevos a judge_eval_cases.jsonl
python3 evals/run_judge_evals.py            # 3. ya corre con los casos nuevos incluidos, sin tocar este script
```

`judge_agent.py` persiste el `payload` de cada corrida junto al veredicto en `logs/judge_verdicts.jsonl` (antes solo se guardaba el resultado) — redactado con el mismo `firewall_proxy._redact()` que usa el firewall, porque ni la descripción cruda del ticket ni el diff real que aplica Copilot pasan por esa redacción antes de llegar al juez. `logs/judge_reviews.jsonl` guarda las revisiones humanas (`human_agreed`, `human_expected_verdict`, nota opcional) — ambos archivos están en `.gitignore` igual que el resto de `logs/*.jsonl`.

### 13. Orquestación real con Prefect (alternativa a run_poc_loop.sh)

`run_poc_loop.sh` sigue funcionando igual que siempre — es la forma más simple de correr esto. Pero si querés reintentos automáticos por paso, estado persistido entre corridas, y una UI para ver cada ejecución como un grafo en vez de leer texto de terminal, `orchestration.py` corre exactamente los mismos building blocks (`jira_client.py`, `sonar_client.py`, `cypher-shell`, el firewall, `gh`/`git`, `run_module_tests.sh`, `judge_agent.py`) pero orquestados por **Prefect**:

```bash
docker compose up -d prefect-server
# la primera vez, descargá el modelo de tu .env si no lo hiciste:
pip install -r requirements.txt
export PREFECT_API_URL=http://localhost:4200/api
python3 orchestration.py
```

Abrí `http://localhost:4200` para ver la corrida completa como un grafo — qué paso falló, cuánto tardó cada uno, y reintentos automáticos en los pasos que fallan por motivos transitorios (red, servicios que tardan en levantar). Usa las mismas variables de `.env` que `run_poc_loop.sh`.

### 13.1. Por qué no migrar a un framework de agentes (LangGraph/CrewAI/AutoGen/Agno)

Decisión de arquitectura evaluada explícitamente (no por default): **se mantiene la orquestación propia.**

- **El problema real de este repo no es de orquestación** — ya existe: `run_poc_loop.sh`/`orchestration.py` son un grafo de etapas con gates reales (firewall rechaza → corta; tests fallan → corta antes del juez; juez `FLAGGED` → bloquea o reintenta una vez). `orchestration.py` sobre Prefect (§13) ya da retries, estado persistido y una UI de grafo — exactamente lo que **LangGraph** ofrecería, expresado en otro DSL. Migrar sería reescribir la misma máquina de estados sin ganar una gate, un retry, ni una vista que no exista ya.
- **No hay colaboración entre agentes que orquestar.** **CrewAI**/**AutoGen** resuelven agentes que negocian/delegan entre sí. Acá el coding agent y el juez son deliberadamente independientes y adversariales (el juez audita sin ver el razonamiento del coding agent, y nunca puede revertirlo) — es una decisión de gobernanza, no una limitación técnica. Un framework multi-agente empujaría hacia más acoplamiento entre ellos, en la dirección contraria a lo que la seguridad de este diseño necesita.
- **Agno** apunta a prototipos livianos — este repo ya superó esa etapa (retries, secrets, rate limiting, logging estructurado, grafo de conocimiento en Neo4j).
- **Lo que faltaba, se construyó con el mismo patrón, no con un framework externo**: `epic_planner.py` (§6.1) ya reordena historias por dependencia real y detecta conflictos en vez de concatenar mecánicamente; `graph_writer.py` (§8.1) ya da al juez/coding agent historial real de riesgo por componente vía Neo4j, un paso hacia gestión de riesgo menos puramente reactiva (aunque el juez sigue evaluando *después* de que el diff existe — no hay un paso de pre-chequeo antes de escribir código). Ambos usan el mismo patrón que ya tenían `coding_agent.py`/`judge_agent.py` (`agent_loop.py` compartido, tools locales + MCP, confirmación humana) — la evidencia de que este patrón escala a nuevos agentes sin adoptar un framework externo ya no es solo una promesa, es lo que pasó.

**Cuándo reconsiderar esto**: si en algún momento se necesitan agentes que genuinamente negocien entre sí (no es el caso hoy), LangGraph sería la opción más alineada por ya pensar en grafos de estado — pero no antes de que exista ese problema real.

### 14. Chat interactivo (`chat.py`) — explorar y actuar sobre la PoC/el repo objetivo a mano

Distinto de `docker exec -it poc-ollama ollama run llama3.1` (Ollama pelado, sin nada de este proyecto): `chat.py` reusa exactamente la misma infraestructura que `coding_agent.py`/`judge_agent.py` — el mismo backend dual con fallback en vivo (`agent_loop.py`), **todas** las tools locales de `coding_agent.py` (leer/escribir/editar archivos, listar, grep, git diff/log, detectar stack, consultar Sonar — con la misma confirmación humana `[s/n]` antes de escribir/ejecutar, que acá sí se responde en vivo porque lo corrés en tu propia terminal), y los mismos MCP reales (Neo4j-cypher, Qdrant-rag).

```bash
python chat.py                      # repo objetivo = directorio actual
python chat.py /ruta/a/otro/repo    # o un repo puntual
```

Charla libre, sin JSON estructurado de salida — para explorar el grafo/código/Sonar interactivamente, o pedirle cambios puntuales sobre un repo con el mismo nivel de supervisión que ya tiene el pipeline. No es parte del pipeline auditado: no escribe en `logs/*.jsonl`, no exige un ticket de Jira. Escribí `salir`/`exit`/`quit` o `Ctrl+C` para cortar.

## Troubleshooting / FAQ

**`docker compose up` no levanta SonarQube / se reinicia en loop.**
En Linux/WSL falta el `vm.max_map_count` que pide Elasticsearch (motor de búsqueda interno de SonarQube): `sudo sysctl -w vm.max_map_count=524288` (ver §0). Se resetea al reiniciar la VM de WSL2 — si vuelve a pasar tras un reinicio de Windows, volvé a correrlo.

**`ai-firewall` responde 401 en `/evaluate`.**
Seteaste `FIREWALL_API_KEY` pero el script no está mandando el header, o al revés: el firewall no tiene la key pero vos esperás que la exija. Confirmá que `FIREWALL_API_KEY` en `.env` sea la misma que usa `run_poc_loop.sh`/`orchestration.py`, y que reiniciaste `ai-firewall` después de setearla (`docker compose restart ai-firewall`).

**Un MCP (`neo4j-cypher`, `qdrant-rag`, `azure-devops`) no arranca en VS Code / Copilot Chat.**
Necesitan `uv`/`uvx` (los dos primeros) o Node.js (`npx @azure-devops/mcp`) instalados **en el host**, no en un contenedor — revisá `.vscode/mcp.json` y que esos binarios estén en el `PATH` que ve VS Code. El agente juez (`judge_agent.py`) usa las mismas dependencias para conectarse por stdio; si falla ahí, es la misma causa.

**El MCP `atlassian` no conecta / pide login todo el tiempo.**
Es remoto (`mcp.atlassian.com`) y usa OAuth por navegador — la sesión puede expirar. No tiene relación con `JIRA_API_TOKEN` (esa variable es solo para `jira_client.py`, el pipeline scripteado). Si Copilot Chat lo sigue pidiendo, revisá que la cuenta con la que iniciás sesión tenga acceso a la instancia Jira/Confluence correcta.

**El juez o `coding_agent.py` no encuentran `ANTHROPIC_API_KEY` ni Ollama.**
Sin `ANTHROPIC_API_KEY` en `.env`, ambos intentan `OLLAMA_URL` (default `http://localhost:11434`). Si usás el `ollama` del `docker-compose.yml`, acordate de bajar un modelo con tool-calling una sola vez: `docker exec poc-ollama ollama pull llama3.1` (o el que hayas puesto en `OLLAMA_MODEL`). Sin ninguno de los dos, el juez se omite (no bloquea) y `coding_agent.py` no corre — cae a `gh copilot suggest` (Camino B2).

**`run_poc_loop.sh` se niega a arrancar con "working tree sucio".**
Es intencional: el pipeline no quiere mezclar tus cambios sin commitear con lo que aplique el agente. Hacé `git commit` o `git stash` en el repo objetivo (no en `poc-ai-agents`) antes de correr.

**El ticket trae datos viejos aunque lo edité en Jira.**
`jira_client.py` cachea lecturas por `CACHE_TTL_SECONDS`. Borrá `cache/` para forzar una lectura en vivo: `rm -rf cache/`.

**Falco no arranca o no genera eventos en Windows.**
Ver §10 — necesita el probe moderno de eBPF, que depende del kernel de la VM WSL2 de Docker Desktop. Si te bloquea, comentá el servicio `falco` en `docker-compose.yml`; el resto del pipeline sigue funcionando sin monitoreo a nivel de sistema.

**El pipeline dice "árbol sucio" contra un repo objetivo que a vos te parece limpio (Windows + Docker-outside-of-Docker).**
Ver §6.1.1 -- el config `core.autocrlf` a nivel SYSTEM de Git para Windows (invisible a `--global`/`--local`) suele convertir a CRLF al hacer checkout, mientras los blobs guardan LF; un contenedor Linux viendo el mismo bind mount encuentra cada línea como "modificada". `scripts/run_epic_dood.sh` ya lo resuelve solo; si armás tu propio comando `docker run` a mano, corré `git config core.autocrlf false` en el clon ANTES de cualquier `checkout`/`reset --hard`.

**Un archivo `cache/*.json` aparece commiteado en el diff/PR real del repo objetivo.**
`CACHE_DIR` en `.env` tiene que quedar **vacío** — `cache_utils.py` ancla el default al directorio del propio módulo (`repo/cache`), no al `cwd` del proceso que lo importa. Un valor tipo `./cache` se resuelve contra el `cwd` de quien corra el pipeline; dentro de Docker-outside-of-Docker eso es el repo OBJETIVO, no este repo, y termina commiteando el cache real del pipeline como si fuera parte del cambio. Confirmado real: pasó en una PR real de Azure DevOps.

## Limitaciones reales

- **No detecta bugs por su cuenta**: la detección del problema sigue siendo humana (alguien escribe el ticket con evidencia real). El pipeline no monitorea microservicios en runtime.
- **El coding agent en la nube (Camino A) no ve tu infraestructura local**: el grafo/Sonar viajan como texto precalculado en el Issue, no se consultan en vivo desde GitHub.
- **`gh copilot suggest` (Camino B2) es un fallback débil**: sugerencia de un solo tiro, sin memoria ni herramientas, para cuando no hay `ANTHROPIC_API_KEY` ni Ollama.
- **El testing agent no cubre UI real de apps móviles** (Expo/React Native/Ionic) sobre emulador/dispositivo — Detox/Maestro/Espresso necesitan virtualización de hardware, frágil dentro de Docker Desktop. Solo corre los tests unitarios/lógica de esos proyectos.
- **Falco es poco confiable en Windows/Docker Desktop** por la dependencia del kernel de la VM WSL2 — funciona de forma más sólida en un host Linux nativo.
- **Las épicas solo funcionan si todos los componentes hijos viven en el mismo repo** — el pipeline se niega a trabajar (no intenta "a medias") si detecta o no puede confirmar lo contrario.
- **El juez puede omitirse sin bloquear la corrida** si no hay backend de modelo disponible — es una segunda opinión, no un gate obligatorio como el testing agent.
- **No es un framework reusable fuera de este repo**: los building blocks (`jira_client.py`, `sonar_client.py`, `agent_loop.py`, etc.) están escritos para este pipeline específico, no como una librería genérica.
- **Algunas llamadas de red vía `git`/`gh` en `orchestration.py` (push, `gh pr create`, `git pull`) no tienen timeout explícito todavía** -- si el remoto se cuelga, pueden mantener una task de Prefect abierta indefinidamente sin una señal distinta de "timeout" (a diferencia del subproceso del juez, que sí tiene `JUDGE_SUBPROCESS_TIMEOUT_SECONDS`). Gap real identificado, no bloqueante para uso normal, pendiente de la misma cota que ya tiene el juez.
- **Ollama solo con modelos chicos (7-9B) tiene un techo real investigando repos desconocidos**, incluso con "thinking" y model-switching (§11.1) -- confirmado en vivo: puede bloquear correctamente (sin inventar) ante código genuinamente ambiguo o ausente, pero no siempre explora tan a fondo como Anthropic antes de rendirse. Modelos más grandes (`qwen3-coder:30b`+) existen pero típicamente superan la memoria default de Docker Desktop (~15-16 GB) -- necesitan subir ese límite a propósito.
