# Qué cambia si esta arquitectura se usa para un agente financiero

Notas sobre qué de `poc-ai-agents` es reutilizable tal cual, qué hay que endurecer, y qué es directamente insuficiente si el dominio pasa de "código" a "finanzas" (trading, pagos, crédito, posiciones, reporting regulatorio).

## 0. El cambio de categoría de riesgo

En esta PoC, el peor caso es código mal escrito que un humano revisa en un PR antes de mergear — hay un colchón. En un agente financiero, el peor caso es dinero movido o una decisión con consecuencia legal (crédito denegado, orden ejecutada, posición cambiada) — **no hay colchón automático**, así que el patrón "firewall con regex + confirmación humana antes de ejecutar" que usamos acá no alcanza como control único.

## 1. Doble control (maker-checker), no un `y/n` en terminal

- `gh copilot suggest` pide confirmación humana antes de correr un comando — eso es un control de una sola persona.
- Cualquier acción financiera irreversible (mover fondos, ejecutar una orden, cambiar un límite de crédito) necesita **dos personas distintas**: quien propone (el agente o quien lo asistió) y quien aprueba, con separación de roles verificable — no la misma persona aceptando su propia sugerencia.
- El equivalente arquitectónico del "coding agent abre un PR" acá sería: el agente arma la propuesta de operación, un segundo humano con autoridad la aprueba explícitamente, y solo entonces se ejecuta contra el sistema real.

## 2. Auditoría inmutable, no `logs/*.jsonl`

- `firewall_audit.jsonl` y `copilot_contribution.jsonl` son archivos locales, editables, sin garantía de integridad — suficiente para una PoC, no para un requisito regulatorio real.
- Un agente financiero necesita logs con integridad verificable (write-once, hash-chained o en un store con control de acceso y retención auditable), alineado a lo que pida el marco aplicable: SOX si cotiza en EE.UU., SR 11-7 (model risk management) si hay modelos tomando decisiones de crédito/riesgo, GDPR/regulación local si hay datos personales de clientes.
- El comentario automático en Jira que agregamos (`jira_client.py comment`) es un buen patrón de "dejar rastro donde alguien lo audita" — el equivalente financiero sería dejar ese rastro en el sistema de registro regulatorio de la firma, no solo en un ticket interno.

## 3. Cero tolerancia a alucinación en cifras

- Un agente de código se equivoca y el PR lo detecta. Un agente financiero que alucina un monto, una tasa o un saldo no tiene ese filtro — el número "suena bien" y puede pasar.
- Cada cifra que el agente use debe venir con **trazabilidad a la fuente exacta** (qué sistema, qué timestamp, qué query) — el patrón de este PoC de "todo dato viene de una API real, nunca de un mock" es el correcto, pero acá además hay que poder **probar** el origen de cada número en la respuesta, no solo que provino de una llamada real.
- El RAG (Qdrant) que usamos para traer contexto semántico es riesgoso en finanzas si no se distingue claramente "esto es un dato duro de una API" vs. "esto es un fragmento recuperado por similitud que puede no ser exacto para este caso puntual".

## 4. El firewall tiene que ser DLP real, no regex

- `firewall_proxy.py` busca `password=`, `secret_key=` y un patrón de clave estilo Azure — heurísticas razonables para credenciales técnicas, insuficientes para datos financieros sensibles: números de cuenta, números de tarjeta (necesita validación tipo Luhn, no solo regex), información material no pública (riesgo de insider trading si un agente la expone fuera de quienes deben verla), PII de clientes bajo el marco de protección de datos que aplique.
- El "egress" necesita reglas específicas del dominio financiero, probablemente un motor de DLP dedicado en vez de expresiones regulares hechas a mano.

## 5. Explicabilidad como requisito, no como bonus

- Si el agente participa en una decisión de crédito, hay marcos (en EE.UU., por ejemplo, adverse action notices) que exigen poder explicar **por qué** se tomó la decisión, en términos que un regulador o un cliente puedan entender — no alcanza con "el modelo lo sugirió".
- Esto empuja a preferir agentes cuyo razonamiento se pueda reconstruir (qué datos vio, qué regla aplicó) sobre agentes de caja negra, incluso a costa de flexibilidad.

## 6. Qué sí se reutiliza tal cual

- El patrón de **ingress antes que egress** (bloquear manipulación del agente antes de procesar cualquier dato) es correcto y aplica igual.
- El patrón de **contexto pre-computado localmente cuando el agente corre en la nube** (porque no puede alcanzar sistemas internos) sigue siendo válido — de hecho es más crítico en finanzas, donde exponer un MCP financiero interno a un agente en la nube sin control de red es un riesgo mayor que en código.
- La idea de **medir cuánto colabora el agente** (`report_sprint_metrics.py`) es trasladable a "cuántas operaciones asistió, cuántas se aprobaron, cuántas se rechazaron" — la métrica de adopción sigue siendo relevante para un VP/comité de riesgo.

## 7. Primera pregunta antes de construir nada

Antes de adaptar esta arquitectura, definir: **¿el agente solo asiste (recomienda, redacta, resume) o puede iniciar acciones (ejecutar, mover, aprobar)?** Todo lo de arriba — doble control, auditoría inmutable, explicabilidad — es mucho más exigente en el segundo caso que en el primero. Vale la pena acotar el primer alcance a "solo asiste" y tratar "puede iniciar acciones" como una fase posterior con su propio análisis de riesgo.
