# Mensajería persistente de Collab e integración con OpenCode

Fecha: 2026-09-09. Estado: especificación aprobada por el usuario el 2026-09-10 al confirmar continuar con la implementación. Implementación y validación pendientes.

## Objetivo

Entregar mensajes entre agentes sin intervención manual para leerlos, conservando pendientes ante cierres de OpenCode, reinicios del servidor y desconexiones. Reutilizar las sesiones OpenCode existentes y distinguir persistencia, entrega a la sesión, acuse del agente y ejecución de tareas.

## Decisiones y alcance

- Ampliar Collab y su SQLite existente; añadir un plugin OpenCode. No introducir un broker adicional ni otro servicio de mensajería independiente.
- Un servidor de cola por entorno de colaboración, elegido mediante configuración; no fijarlo a ninguna máquina. Su proceso vive independientemente de OpenCode.
- Identidad estable por agente y proyecto, independiente de las salas y de las sesiones OpenCode.
- Modo automático o solo notificación configurable por identidad.
- OpenCode cerrado: conservar pendientes y recuperarlos al abrir y vincular la sesión. No arrancarlo automáticamente.
- Solicitudes y respuestas pueden activar turnos; acuses, presencia y cambios de estado no los activan.
- Primera versión: texto y referencias a tareas o archivos. La transferencia de archivos continúa por Collab; esta especificación no garantiza disponibilidad duradera del archivo referenciado.
- Fuera de alcance: migración entre servidores, alta disponibilidad, ejecución exactamente una vez de acciones externas y borrado automático del historial.

## Base existente y carencias observadas

Revisión de código sobre HEAD `d1cbff9`:

- `src/collab/server/store.py`: tablas SQLite de eventos, participantes y salas; secuencia para recuperar el flujo.
- `src/collab/client/inbox.py`: buzón local persistente y cursor de recepción.
- `src/collab/cli.py`, `cmd_send`: envío directo al hub; devuelve error al fallar, sin bandeja de salida persistente en ese flujo.
- `src/collab/client/hub_client.py`, `send`: genera un ID A2A nuevo por llamada; los reintentos duraderos necesitarán conservar una identidad de mensaje estable.
- `ROADMAP.md`: integración con sesiones OpenCode existentes planificada.

El hub local se observó offline. Eso demuestra indisponibilidad, no pérdida del historial. La implementación debe verificar la recuperación existente antes de atribuir pérdidas a SQLite o sustituir mecanismos que ya funcionan.

## Arquitectura

| Componente | Responsabilidad e interfaz |
| --- | --- |
| Servidor de cola Collab | Persistir mensajes, destinatarios, secuencias y acuses; aceptar envíos idempotentes; ofrecer recuperación y reserva de consumo. Depende de SQLite y del transporte de Collab. |
| Cliente local Collab | Guardar salidas antes del envío, reintentar, recuperar pendientes y mantener estado duradero. Expone operaciones locales al plugin y a la CLI. |
| Plugin OpenCode | Vincular sesión y buzón, observar actividad, notificar o entregar tandas y reconciliar entregas. Depende de interfaces del cliente y del SDK OpenCode. |
| Herramientas del agente | Enviar, responder, consultar y acusar IDs concretos. Los resultados de tareas se envían como respuestas relacionadas. |

El plugin usa interfaces del cliente; no modifica directamente las bases privadas del servidor. La configuración y los registros del plugin residen fuera de los repositorios de aplicaciones. Cliente y plugin deben funcionar en macOS y Linux.

### Identidad y vinculación

Cada buzón tiene un ID interno estable y un nombre legible, por ejemplo `mac/ios-jarvis`. La vinculación contiene servidor, buzón, instancia OpenCode, ID de sesión y directorio del proyecto. Cambiar la URL del servidor no migra datos ni identidades automáticamente.

Solo una sesión consume activamente un buzón. El servidor otorga una reserva temporal renovable con una generación identificable; las operaciones con una reserva caducada o reemplazada se rechazan. Al desconectarse, el plugin suspende nuevas entregas hasta recuperar una reserva válida.

Al abrir OpenCode se puede restaurar una vinculación previamente autorizada si coinciden sesión y directorio. Una sesión distinta requiere vinculación explícita. Si otra sesión mantiene una reserva válida, se informa del conflicto en lugar de consumir en paralelo.

Las salas agrupan conversaciones. Cerrarlas no elimina los mensajes ya aceptados ni sus destinatarios pendientes. La persistencia de los buzones no depende de la vida del proceso que creó la sala.

### Contrato lógico del mensaje

Cada mensaje contiene:

- ID único generado y guardado en origen antes del primer envío.
- Identidad autenticada del emisor y destinatario estable.
- Tipo: solicitud, respuesta o informativo.
- Texto, fecha de origen y referencia de conversación; referencia al mensaje respondido cuando corresponda.
- Secuencia de buzón y fecha de aceptación asignadas por el servidor.

Los acuses y eventos de presencia/estado son eventos de control, no solicitudes de conversación. Una referencia a tarea o archivo es contenido identificable, no una orden de ejecutarlo ni una copia del recurso.

La deduplicación usa emisor autenticado e ID de mensaje. Reutilizar ese ID con contenido o destinatario distinto produce un conflicto visible. La aceptación persiste mensaje y destinatario antes de responder. Si se usa una sala como destino, sus destinatarios se fijan al aceptar el mensaje; cada destinatario conserva su propio estado de entrega y acuse.

## Ciclo de vida y garantías

1. **Pendiente de envío:** el cliente confirma que está encolado solo después de guardarlo localmente.
2. **Aceptado por el servidor:** mensaje y destinatarios están guardados en SQLite. El cliente puede cerrar el reintento de transporte.
3. **Entregado a OpenCode:** se verificó su incorporación a la sesión vinculada; no equivale a comprensión o ejecución.
4. **Confirmado por el agente:** una llamada explícita de herramienta acusa los IDs recibidos. No equivale a tarea terminada.

La ejecución de una solicitud se registra mediante respuestas o el estado de su tarea, separado de estos estados de entrega. Acusar no genera otro turno.

La garantía es entrega al menos una vez con deduplicación. Los reintentos usan espera creciente, variación aleatoria y el mismo ID. Los pendientes no caducan automáticamente. Confirmar retira el mensaje de pendientes, conservando historial y transiciones; la primera versión no purga automáticamente.

El servidor ordena por secuencia de buzón, según aceptación. No se promete orden global entre máquinas ni orden por relojes de origen. Las tandas respetan esa secuencia; los eventos informativos se registran sin activar un turno por sí solos.

No se promete ejecución exactamente una vez de efectos externos. Una operación que deba resistir repeticiones necesita idempotencia propia. La reserva del buzón tampoco detiene una acción externa ya iniciada por una sesión anterior.

## Entrega a OpenCode

El plugin usa el SDK y eventos de estado de OpenCode para dirigirse al ID vinculado. Debe verificar la compatibilidad y semántica real de la versión instalada antes de implementar el adaptador; la documentación consultada describe eventos `session.status`/`session.idle`, acceso al historial y envío a sesiones concretas.

| Situación | Comportamiento |
| --- | --- |
| OpenCode cerrado | Esperar y recuperar al abrir y vincular. |
| Sesión libre, modo automático | Agrupar solicitudes y respuestas pendientes y activar un turno en la misma sesión. |
| Sesión ocupada | Esperar al fin del turno; no interrumpirlo. |
| Solo notificación | Mostrar pendientes; procesar una tanda por acción explícita del usuario. |
| Sesión eliminada o directorio distinto | Suspender y solicitar vinculación; conservar pendientes. |
| Reserva inválida o conexión incierta | Suspender nuevas entregas hasta reconciliar y renovar. |

Solo hay una entrega activa por buzón. La agrupación tiene ventana breve y límites configurables de cantidad y tamaño; el exceso sigue pendiente. La implementación debe fijar valores predeterminados y probar sus límites. No se trunca contenido silenciosamente.

Cada entrega incluye origen, tipo e IDs, presentada como comunicación de otro agente y no como instrucción del usuario. Conserva permisos y restricciones de la sesión; el contenido de pares no amplía autoridad.

### Registro y reconciliación

Antes de invocar OpenCode se persiste un registro de intento con buzón, IDs, sesión, directorio y correlación de tanda. La incorporación a OpenCode conserva esa correlación de forma consultable en el historial. Tras un fallo se consulta la sesión antes de repetir un intento dudoso.

Si el historial demuestra incorporación, se recupera el estado entregado. Si demuestra ausencia y el intento anterior ya no puede completarse, puede reintentarse. Si no se puede determinar el resultado, se suspende la entrega afectada y se muestra el estado incierto, evitando reenvíos ciegos. La compatibilidad del SDK con esta reconciliación es una condición de aceptación, no una garantía asumida.

La ausencia de acuse tras una entrega comprobada no dispara turnos repetidos automáticamente: el mensaje sigue pendiente de acuse y puede revisarse o reintentarse explícitamente. Así se separa reintento de transporte de repetición de trabajo del agente.

### Cancelación y bucles

Cancelar un turno de Collab no lo reactiva inmediatamente. Los mensajes sin acuse permanecen pendientes de revisión o reintento explícito; los confirmados conservan su estado aunque el trabajo no haya terminado.

Una respuesta final puede acusarse sin enviar otra respuesta. Un límite configurable de activaciones consecutivas sin intervención humana pausa la entrega automática y muestra los pendientes. Los acuses y eventos de control nunca cuentan como motivo para activar al modelo. El contador se recupera tras reinicios y se restablece con intervención explícita del usuario.

### Controles

Consultar servidor, conexión, vinculación, modo y pendientes; procesar una tanda; pausar/reanudar; desvincular. Pausar o desvincular no borra mensajes. La recepción automática se elige por identidad; una vinculación nueva solicita ese modo explícitamente.

## Recuperación y errores

- Reinicio del servidor: recuperar SQLite, reanudar clientes y renovar reservas; una reserva de una ejecución anterior no autoriza por sí sola una nueva entrega.
- Reinicio del cliente/plugin: recuperar bandeja de salida y registros de intentos; reconciliar antes de entregar otra vez.
- Confirmación de transporte perdida: reenviar mismo ID y devolver aceptación existente.
- Credenciales rechazadas: conservar pendientes y mostrar error hasta corregir conexión.
- Disco lleno o escritura fallida: no confirmar aceptación; registrar el error cuando sea posible y mostrarlo al emisor.
- Sesión cerrada durante entrega: conservar el intento y reconciliar al recuperar la vinculación, sin elegir otra sesión automáticamente.

Se reutiliza la autenticación de Collab para verificar emisor y acceso al buzón. Reservas y acuses solo afectan al destinatario autorizado. Credenciales e invitaciones no aparecen en mensajes, diagnósticos ni documentos.

## Diagnóstico

Por identidad: servidor, conexión, sesión/directorio, modo/pausa, cantidades por estado, antigüedad del pendiente más antiguo, último error, próximo reintento y motivo de bloqueo. Por mensaje: IDs correlacionados y transiciones verificables. Mostrar separadamente entrega incierta, pendiente de acuse y tarea pendiente.

## Validación y aceptación

Combinar pruebas automatizadas de persistencia, exclusión, fallos y límites con dos sesiones reales OpenCode en macOS y Linux. Acordar la participación de la otra instancia por Collab; las intervenciones en el backend de Jarvis corresponden exclusivamente a su instancia.

1. Enviar con servidor apagado, reiniciar cliente y comprobar entrega posterior desde la salida persistida.
2. Enviar con OpenCode cerrado, abrirlo y recuperar pendientes tras vincular.
3. Reiniciar servidor después de guardar y antes de confirmar: reintento produce un solo mensaje lógico por destinatario.
4. Entregar a sesión libre conservando ID; una sesión ocupada termina el turno antes de recibir la tanda. Incluir carrera con entrada humana.
5. Modo notificación y eventos técnicos: cero invocaciones al modelo sin acción explícita o mensaje activador.
6. Interrumpir plugin antes y después de incorporación a OpenCode: reconciliar historial; si es imposible, mostrar incertidumbre sin reenvío ciego.
7. Dos sesiones disputan buzón: una sola consume; reservas caducadas no autorizan nuevas confirmaciones. Probar también partición de red.
8. Cancelar turno: no se reactiva; preservar estados de mensajes acusados y no acusados.
9. Cerrar sala y reiniciar servicio: sus mensajes pendientes siguen recuperables.
10. Ráfagas: orden, agrupación, límites y exceso pendiente; acuses no activan; bucle provoca pausa y sobrevive al reinicio.
11. Fallar escrituras y rechazar credenciales: no afirmar envío/aceptación y conservar lo ya persistido.
12. Reutilizar ID con carga distinta: conflicto visible, sin sobrescribir el original.

Guardar comandos, resultados, IDs de mensajes/sesiones y transiciones. Una afirmación del modelo no sustituye una llamada de acuse ni evidencia de ejecución. Medir turnos de entrega y agrupación; no atribuir ahorro frente a sondeo sin comparar mediciones equivalentes.

## Próximo paso

Revisión del documento por el usuario. Después de su aprobación, usar `writing-plans` para desglosar interfaces, migraciones compatibles, valores predeterminados, adaptador OpenCode, pruebas y despliegue coordinado. No se ha implementado ni probado este diseño.

## Referencias

- `ROADMAP.md`
- `src/collab/server/store.py`
- `src/collab/client/inbox.py`
- `src/collab/client/hub_client.py`
- `src/collab/cli.py`
- https://opencode.ai/docs/plugins/
- https://opencode.ai/docs/sdk/
