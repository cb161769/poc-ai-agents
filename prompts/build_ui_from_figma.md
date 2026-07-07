# Prompt: construir/ajustar una pantalla real desde Figma

Pega este prompt en Copilot Chat (VS Code) con el MCP `figma-dev-mode` activo
(ver `.vscode/mcp.json`). Requiere tener **Figma Desktop** abierto, con el
archivo de diseño real cargado, y "Enable local MCP Server" activado en
Preferencias → habilita el servidor en `http://127.0.0.1:3845/sse` — no hace
falta ningún token en `.env`, el servidor local ya tiene el contexto del
archivo abierto en el momento.

---

Tenés acceso a la herramienta MCP `figma-dev-mode`, que expone las
especificaciones reales del archivo de Figma que tengo abierto ahora mismo
(medidas, colores, tipografía, componentes, tokens de diseño).

1. Usá las tools de `figma-dev-mode` para inspeccionar el frame/componente
   que te indique (por nombre o selección actual en Figma).
2. Extraé los valores reales — no inventes colores ni espaciados — y
   aplicalos a `sample-repo/frontend/public/index.html` /
   `sample-repo/frontend/public/login.js` (o el archivo que corresponda al
   ticket).
3. Si el diseño no coincide con la implementación actual, decime
   explícitamente qué cambiaste y por qué, citando el valor exacto que
   tomaste de Figma (ej. "el botón usa #1F4E8C según el estilo `primary/600`
   del archivo").
4. Después de aplicar el cambio, recordame correr el testing agent real
   (`./scripts/run_module_tests.sh Frontend`) para confirmar que el test de
   Playwright sigue pasando con el nuevo markup.

No inventes specs que no puedas verificar con la tool — si un valor no está
en el archivo de Figma, decilo en vez de asumir un default.
