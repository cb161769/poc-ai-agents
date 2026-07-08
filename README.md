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

**Sobre `repository_origen`** (qué componente afecta el ticket): el pipeline primero mira el campo nativo **Components** del ticket de Jira (Settings → Components de tu proyecto), con fallback a labels si no hay match ahí. El set de componentes válidos se deriva automáticamente de los nombres de nodo reales en Neo4j en cada corrida — `JIRA_KNOWN_COMPONENTS` en `.env` es solo el fallback para cuando Neo4j todavía no está levantado.

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

Opcional pero recomendado: seteá `FIREWALL_API_KEY` en `.env` antes de levantar `ai-firewall` para que `/evaluate` exija el header `X-Firewall-Key` (401 sin él) — sin esto, cualquiera que llegue a `FIREWALL_URL` puede pegarle al firewall directo, sin pasar por Jira. `run_poc_loop.sh`/`orchestration.py` mandan el header automáticamente si la variable está seteada.

## 3. Sincronizar el grafo desde Azure DevOps real (opcional pero recomendado)

Abre VS Code en este directorio (ya trae `.vscode/mcp.json`), confirma que los cinco MCP (`neo4j-cypher`, `azure-devops`, `qdrant-rag`, `atlassian`, `figma-dev-mode`) arrancan, y pega el contenido de [prompts/sync_graph_from_azure_devops.md](prompts/sync_graph_from_azure_devops.md) en Copilot Chat. Si no lo haces, el grafo usa el seed estático de `seed/seed.cypher`.

Para construir/ajustar pantallas reales con specs de Figma (medidas, colores, tokens), abre **Figma Desktop** con el archivo de diseño, activa "Enable local MCP Server" en Preferencias, y pega [prompts/build_ui_from_figma.md](prompts/build_ui_from_figma.md) en Copilot Chat — no requiere token, el servidor local ya tiene el contexto del archivo abierto. Esta vía es **interactiva**: un humano decide qué frame mirar.

**Vía automatizada (sin Figma Desktop abierto)**: si la descripción del ticket de Jira trae un link real de Figma (`figma.com/file/...` o `figma.com/design/...?node-id=...`), y seteaste `FIGMA_API_TOKEN` en `.env` (Personal Access Token real, distinto del MCP), el pipeline (`run_poc_loop.sh`/`orchestration.py`) lo detecta solo y `figma_client.py` le pega a la REST API real de Figma para sumar esas specs (colores/medidas/texto del nodo indicado) al prompt compuesto — igual que ya hace con el grafo de Neo4j y los hallazgos de Sonar. Sin link en el ticket o sin la variable seteada, esta sección simplemente se omite, sin bloquear la corrida.

El MCP `atlassian` es remoto (`mcp.atlassian.com`) y usa OAuth: la primera vez que Copilot Chat lo invoque, VS Code abrirá el navegador para que inicies sesión en tu instancia Jira/Confluence. No requiere `JIRA_API_TOKEN` — esa variable es solo para `jira_client.py`, que sigue siendo el que alimenta `run_poc_loop.sh` (el MCP es para que Copilot explore Jira interactivamente, no para el pipeline scripteado).

## 4. Pararte en el repo real que corresponde al ticket

`run_poc_loop.sh`/`orchestration.py` ya **no** usan un repo de ejemplo: operan sobre el repositorio git en el que estás parado (`cd`) al invocarlos, detectado con `git rev-parse --show-toplevel`. Antes de correr el paso 6:

```bash
cd /ruta/a/tu-proyecto-real   # el repo al que corresponde el ticket de Jira
```

El pipeline se niega a arrancar si no estás dentro de un repo git, o si el working tree tiene cambios sin commitear (para no mezclar tu trabajo en progreso con lo que aplique Copilot) — hacé commit o `git stash` antes de correr.

`sample-repo/` sigue en este proyecto solo como referencia de qué archivo de proyecto necesita cada stack (`pom.xml`, `go.mod`, `package.json`, etc.) para que la auto-detección de `scripts/run_module_tests.sh` lo reconozca — no es el default de nada.

## 5. Verificar que todo está listo

```bash
./scripts/check_prereqs.sh
```

## 5.1. Smoke test end-to-end (opcional, recomendado tras cualquier cambio)

`check_prereqs.sh` valida que los servicios estén arriba; esto va un paso más allá y corre el pipeline real de punta a punta, sin mocks:

```bash
./scripts/smoke_test.sh
```

Crea un **ticket Jira real y descartable** (etiquetado `smoke-test`, cerrado automáticamente al final), un **repo git temporal** limpio, y corre el `run_poc_loop.sh` real contra ambos — validando en serio la lectura de Jira, la consulta al grafo, los hallazgos de Sonar y la evaluación del firewall (con su comentario y transición reales en el ticket).

**Qué NO cubre, a propósito**: la etapa 5 (coding agent) queda afuera — `gh copilot suggest` es una TUI interactiva que no se puede automatizar sin volverse frágil o mockear (algo que este proyecto evita), y el coding agent en la nube necesita un repo GitHub real y abre el PR de forma asíncrona. El ticket sintético se cierra automáticamente al terminar; si querés validar también la etapa 5, corré `./run_poc_loop.sh` a mano (sin `SMOKE_TEST_MODE`) con un ticket real propio.

## 6. Correr el flujo completo

```bash
./run_poc_loop.sh                # usa JIRA_TICKET_KEY de .env
./run_poc_loop.sh JIRA-123       # o cualquier ticket real, pasado como argumento
# equivalente con orchestration.py: python3 orchestration.py [JIRA-123]
```

El escenario (limpio o malicioso) lo decide el **contenido real** del ticket de Jira, no un flag — y no estás atado a un solo ticket fijo en `.env`: pasale cualquier ticket que le compartas a Copilot como primer argumento.

**La detección del bug sigue siendo tuya**: nada acá monitorea microservicios en runtime. El script chequea si la descripción trae un bloque de código real (campo estructurado del ADF de Jira, no un regex adivinando palabras) — si pegaste el log como texto plano en vez de bloque de código, no cuenta como evidencia, y el script comenta en el ticket pidiéndolo bien formateado, mencionando el microservicio exacto — sin frenar la corrida.

Si el bug tiene un video/imagen adjunto y tu instancia tiene **Rovo** activo, el script lee la descripción que Rovo ya haya dejado como comentario (ajustá `ROVO_AUTHOR_NAME_MATCH` si tu instancia lo muestra con otro nombre) y la suma al prompt — sin descargar el video ni correr visión propia. Si el adjunto todavía no tiene descripción de Rovo, el script se detiene antes de llegar al firewall y pide revisión humana.

Apenas el firewall aprueba, el ticket se mueve automáticamente al estado que definas en `JIRA_IN_PROGRESS_STATUS` (default `"In Progress"`) — si tu workflow usa otro nombre, ajustalo en `.env`; si el nombre no matchea ninguna transición disponible, el script avisa pero sigue.

**Importante — qué es y qué no es "el agente" acá:** las etapas 1-4 son orquestación determinística, no un agente. La etapa 5 tiene tres caminos posibles:

- **A — Con `GITHUB_REPO` configurado en `.env`**: el script crea un Issue en tu repo real con todo el contexto ya armado y lo asigna al **GitHub Copilot coding agent**, que corre en la nube de GitHub con su propio razonamiento y abre un PR cuando termina. Requiere Copilot coding agent habilitado en ese repo (plan Business/Enterprise) y que ya tenga un remote real (`git push`). El agente en la nube **no** tiene acceso a tu Neo4j/Qdrant locales — por eso el impacto del grafo y los hallazgos de Sonar viajan como texto ya calculado dentro del Issue, no se consultan en vivo desde la nube.
- **B1 — Sin `GITHUB_REPO`, con `ANTHROPIC_API_KEY` u Ollama alcanzable**: `coding_agent.py`, un **agente real local** — mismo backend dual (Anthropic primero, Ollama de fallback) y la misma maquinaria de tool-calling que ya usa el agente juez (`agent_loop.py`). Razona en varios turnos, con herramientas reales confinadas al repo objetivo: leer/escribir/listar archivos, buscar texto (`grep`), correr comandos de shell, y los mismos MCP del juez (Neo4j-cypher, Qdrant-rag) para consultar el grafo/código histórico por su cuenta. **Cada escritura de archivo y cada comando piden confirmación humana antes de ejecutarse** — se ven y se responden en vivo en la terminal (`[s/n]`), nunca actúa sin supervisión.
- **B2 — Sin `GITHUB_REPO` y sin ningún backend de modelo**: cae a `gh copilot suggest` sobre el prompt saneado — una sugerencia de un solo tiro, sin memoria ni loop, con la misma confirmación antes de ejecutar. Es el fallback de siempre, para quien no tiene `ANTHROPIC_API_KEY` ni Ollama corriendo.

En B1 y B2, el resultado (si hay cambios) se commitea en una rama nueva `copilot/<ticket>-<timestamp>` dentro del repo real detectado en el paso 4, **nunca en la rama que tenías checked out**.

```bash
# revisar el resultado del camino B (B1 o B2), desde el repo real:
git diff <rama-base-real>..copilot/<rama-que-te-haya-mostrado-el-script>
```

## 6.1. Trabajar con épicas

```bash
./run_poc_loop.sh --epic EPIC-123
# equivalente: python3 orchestration.py --epic EPIC-123
```

En vez de un ticket individual, trae la **épica completa + todas sus historias hijas** (vía JQL — por default `parent = "{epic_key}"`, el campo estándar en proyectos "team-managed"; si tu proyecto es "company-managed" y todavía usa el campo custom `Epic Link`, ajustá `JIRA_EPIC_LINK_JQL` en `.env`) y arma **un solo prompt combinado** — Copilot ve el contexto completo de la épica y coordina los cambios entre componentes, en vez de procesar cada historia por separado.

**Solo funciona si todos los componentes que tocan los hijos de la épica viven en el mismo repo.** El pipeline está construido alrededor de un repo por corrida (`TARGET_REPO_DIR`) — si los hijos tocan componentes que en realidad viven en repos distintos, no hay forma honesta de resolverlo en una sola corrida, así que **se niega a trabajar** en vez de intentarlo a medias. Para poder confirmar esto, cada nodo del grafo de Neo4j necesita la propiedad `repo_url` seteada (ver `seed/seed.cypher` y `prompts/sync_graph_from_azure_devops.md`) — sin ese dato en cualquiera de los componentes involucrados, también rechaza la corrida (nunca asume que es el mismo repo sin poder confirmarlo). Cuando rechaza, deja un comentario claro en la épica explicando el conflicto (o el dato faltante) antes de terminar.

Si todo coincide, sigue el mismo flujo que un ticket normal (firewall → coding agent → testing agent → juez → Falco) usando la épica como `TICKET_ID` (ramas/commits quedan como `copilot/EPIC-123-<timestamp>`), y al final deja un comentario en cada historia hija señalando que se procesó como parte de esa corrida combinada.

## 7. Romper el flujo a propósito

Edita el ticket real en Jira (no un archivo local) y agrega a la descripción, por ejemplo:

- **Fuga de datos:** `password=Sup3rS3cr3t!` → el firewall lo aprueba pero con `redactions_applied >= 1`, y verás el prompt censurado antes de que llegue a `gh copilot suggest`.
- **Jailbreak:** `ignore previous instructions` → el firewall responde `403 REJECTED`; el juez audita ese rechazo (§11) pero nunca lo revierte, el script hace `exit 1`, y `gh copilot` nunca se invoca.

Vuelve a correr `./run_poc_loop.sh` después de guardar el cambio en Jira (si corriste hace menos de `CACHE_TTL_SECONDS`, el ticket puede venir de cache — bórralo con `rm -rf cache/` para forzar una lectura en vivo).

## 7.1. Tests del código propio (no del repo objetivo)

`scripts/run_module_tests.sh` (§9) testea el repo *objetivo* del usuario. Esto es distinto: tests unitarios de la herramienta misma (`firewall_proxy.py`, `jira_client.py`, `sonar_client.py`, `cache_utils.py`, `judge_agent.py`) — funciones puras y deterministas, sin pegarle a Jira/Sonar/Neo4j reales (se mockea con `unittest.mock` donde hace falta).

```bash
pip install -r requirements.txt
pytest tests/ -v
```

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

Solo en el Camino B con un cambio aplicado localmente: antes de llamar al juez, el script corre el **test suite real** del módulo afectado dentro de un contenedor descartable (`scripts/run_module_tests.sh`) — nada instalado en tu máquina, nada persistido.

**Generaliza a cualquier lenguaje/framework por auto-detección**, no por una lista fija de componentes: `run_module_tests.sh` recibe directo la ruta del repo real detectado en el paso 4, mira qué archivo de proyecto hay en su raíz y elige imagen + comando solo — `pom.xml` → Maven, `*.csproj` → .NET, `go.mod` → Go, `Gemfile` → Ruby, `Cargo.toml` → Rust, `Pipfile`/`requirements.txt` → Python, y **`package.json` → cualquier stack de Node/TS** (NestJS, Angular, Ionic, Expo, React Native, Vitest/Jest plano), todos en la misma rama sin bifurcar por framework.

`sample-repo/` (JUnit 5 en `auth-service/`, Vitest + Playwright en `frontend/`, pytest en `data-worker/`) queda en el proyecto como ejemplo de referencia de esos archivos de proyecto — no es lo que el pipeline testea por defecto.

Si los tests **fallan**, el pipeline se bloquea ahí mismo (comentario fuerte + transición a `JIRA_BLOCKED_STATUS` + commit `BLOCKED BY TESTS: ...` en la rama) y **el juez ni se llama**. Si pasan, el resultado se le pasa al juez como parte de su contexto — un test que pasa no prueba que su alcance sea suficiente para el cambio real, y el juez lo tiene en cuenta.

Requiere Docker en el host (ya es un prerequisito). Sin Docker, este gate se omite sin bloquear la corrida. **Fuera de alcance**: UI real de apps móviles (Expo/React Native/Ionic) sobre emulador/dispositivo (Detox/Maestro/Espresso) — necesita virtualización de hardware, delicada dentro de Docker Desktop; esta auto-detección solo corre los tests unitarios/lógica de esos proyectos.

## 10. Monitoreo a nivel de sistema (Falco)

Además del firewall/juez (que auditan a nivel *semántico* — qué dice el ticket, qué hace el diff), `falco` monitorea en tiempo real qué hacen los contenedores a nivel de **sistema operativo** (syscalls): shells inesperados dentro de `poc-ai-firewall` o el testing agent, escrituras fuera de las rutas esperadas, conexiones salientes raras desde los contenedores efímeros de test. Reglas propias en [falco/custom_rules.yaml](falco/custom_rules.yaml).

```bash
docker logs -f poc-falco          # alertas en vivo
cat logs/falco_alerts.jsonl       # alertas persistidas
```

**En Windows/Docker Desktop**: Falco necesita que el kernel de la VM WSL2 soporte su probe moderno de eBPF (`--modern-bpf`) — si el contenedor no arranca o no genera eventos, es una limitación de correr Falco fuera de un host Linux nativo, no de esta PoC. Si te da problemas, podés comentar el servicio `falco` en `docker-compose.yml` sin afectar al resto del pipeline.

**Las alertas se correlacionan con cada corrida, no quedan solo en el archivo**: al final de cada corrida (`run_poc_loop.sh`/`orchestration.py`), `scripts/check_falco_alerts.py` filtra `logs/falco_alerts.jsonl` por la ventana de tiempo de esa corrida (desde que el firewall aprobó hasta el final). Si encuentra algo, lo deja como comentario en el ticket de Jira y, si seteaste `FALCO_ALERT_WEBHOOK_URL` (formato compatible con un incoming webhook de Slack), también lo postea ahí. Es puramente informativo — nunca bloquea la corrida por su cuenta, a diferencia del testing agent o el juez.

## 11. Agente juez (segunda opinión, con acceso real a MCP y poder de bloqueo)

Cada corrida — **aprobada o rechazada por el firewall** — pasa además por un **modelo distinto** (Claude, no `gh copilot`) que audita: si el firewall decidió bien, si el cambio real resuelve el ticket, y si la corrida completa tiene sentido.

A diferencia del coding agent (que corre en la nube de GitHub y no puede tocar tu infraestructura local), **el juez corre en tu máquina** — así que se conecta de verdad a `mcp-neo4j-cypher` y `mcp-server-qdrant` por stdio, y puede *verificar* afirmaciones en vez de confiar ciegamente en el texto que le armamos (por ejemplo, consultar el grafo él mismo para confirmar si un cambio realmente no afecta a otros servicios). Si esos MCP no están disponibles (falta `uvx`, Neo4j/Qdrant caídos), el juez sigue funcionando sin herramientas, razonando solo sobre el texto.

**Backend del juez, sin depender de una API paga si no querés**: primero intenta Anthropic (`ANTHROPIC_API_KEY`); si no está configurada, cae al contenedor `ollama` local del `docker-compose.yml` (gratis, offline) — este fallback corre de verdad tanto desde `run_poc_loop.sh` como desde `orchestration.py`. Para que funcione, después de `docker compose up` descargá un modelo con tool-calling una sola vez:
```bash
docker exec poc-ollama ollama pull llama3.1
```
Si cambiás `OLLAMA_MODEL` en `.env`, descargá ese modelo en su lugar. Sin `ANTHROPIC_API_KEY` ni Ollama alcanzable (o ante cualquier falla de red al llamarlos), el juez se omite y la corrida sigue sin veredicto — nunca frena el pipeline por su ausencia.

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
Desde esta versión, cada entrada también trae `backend`, `latency_seconds`, `input_tokens`/`output_tokens` y `estimated_cost_usd` — ver §12.

## 12. Evals: benchmarking del juez y precisión del coding agent

**Precisión del agente juez** — un dataset fijo de casos etiquetados a mano (`evals/judge_eval_cases.jsonl`: tickets + decisión de firewall + diff, cada uno con el veredicto que *debería* dar un humano):
```bash
python3 evals/run_judge_evals.py
```
Corre cada caso contra `judge_agent.judge_with_tools()` de verdad (mismo código que usa `run_poc_loop.sh`/`orchestration.py`), imprime una matriz de confusión (tratando `FLAGGED` como la clase positiva — el error grave es un falso negativo: algo que debía bloquearse y el juez dejó pasar) y el costo/latencia total. Falla (`exit 1`) si hay algún falso negativo. Resultados acumulados en `logs/eval_judge_runs.jsonl`.

**Precisión del coding agent (proxy)** — no hay forma automática de saber "¿resolvió el ticket correctamente?" sin un humano, pero sí hay un piso medible: ¿pasó los tests reales que se supone que tiene que pasar? Eso ya lo agrega `report_sprint_metrics.py` (§8), leyendo el campo `tests_passed` que ahora graba `run_poc_loop.sh` en `copilot_contribution.jsonl` cada vez que el testing agent corre.

## 12.1. Mejora continua del juez (curación humana de evals, no reentrenamiento)

`evals/judge_eval_cases.jsonl` empezó como 7 casos fijos escritos a mano — esto lo hace crecer con corridas reales, sin tocar pesos de ningún modelo (nada acá hace fine-tuning; es curación de un dataset por un humano):

```bash
python3 scripts/review_judge_verdicts.py    # 1. revisa corridas reales pendientes, vos confirmas o corregís el veredicto del juez
python3 scripts/promote_reviews_to_evals.py # 2. las revisiones (acuerdos Y correcciones) se suman como casos nuevos a judge_eval_cases.jsonl
python3 evals/run_judge_evals.py            # 3. ya corre con los casos nuevos incluidos, sin tocar este script
```

`judge_agent.py` ahora persiste el `payload` de cada corrida junto al veredicto en `logs/judge_verdicts.jsonl` (antes solo se guardaba el resultado) — redactado con el mismo `firewall_proxy._redact()` que usa el firewall, porque ni la descripción cruda del ticket ni el diff real que aplica Copilot pasan por esa redacción antes de llegar al juez. `logs/judge_reviews.jsonl` guarda las revisiones humanas (`human_agreed`, `human_expected_verdict`, nota opcional) — ambos archivos están en `.gitignore` igual que el resto de `logs/*.jsonl`.

## 13. Orquestación real con Prefect (alternativa a run_poc_loop.sh)

`run_poc_loop.sh` sigue funcionando igual que siempre — es la forma más simple de correr esto. Pero si querés reintentos automáticos por paso, estado persistido entre corridas, y una UI para ver cada ejecución como un grafo en vez de leer texto de terminal, `orchestration.py` corre exactamente los mismos building blocks (`jira_client.py`, `sonar_client.py`, `cypher-shell`, el firewall, `gh`/`git`, `run_module_tests.sh`, `judge_agent.py`) pero orquestados por **Prefect**:

```bash
docker compose up -d prefect-server
# la primera vez, descargá el modelo de tu .env si no lo hiciste:
pip install -r requirements.txt
export PREFECT_API_URL=http://localhost:4200/api
python3 orchestration.py
```

Abrí `http://localhost:4200` para ver la corrida completa como un grafo — qué paso falló, cuánto tardó cada uno, y reintentos automáticos en los pasos que fallan por motivos transitorios (red, servicios que tardan en levantar). Usa las mismas variables de `.env` que `run_poc_loop.sh`.
