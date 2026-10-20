# Requerimientos — Pipeline ETL de Fórmula 1

> Portafolio de visualización de datos. Fase 1: Fórmula 1. Fase 2 (fuera de alcance de este documento): OpenDota.
>
> Estado: borrador v0.3 · Fecha: 2026-10-19

---

## 1. Objetivo

Construir un pipeline ETL automatizado que extraiga datos de Fórmula 1 de fuentes públicas gratuitas, los transforme en datasets listos para graficar y los publique en un sitio estático en GitHub Pages.

El foco del portafolio es la **presentación de información mediante gráficos**. Por eso este documento empieza por los gráficos (los productos de datos) y deriva hacia atrás qué datos hay que extraer y transformar.

## 2. Arquitectura conceptual

```
GitHub Actions (cron + disparo manual)
  ├─ EXTRACT    OpenF1 (2023+) y Jolpica-F1 (1950+)
  ├─ RAW        respuestas crudas + metadatos de extracción
  ├─ TRANSFORM  limpieza, tipado, dimensiones conformadas, métricas derivadas
  ├─ QUALITY    validaciones de datos (si fallan, NO se publica)
  └─ LOAD       datasets "gold" (uno por gráfico) + manifiesto
                     │
                     ▼
GitHub Pages (estático) → el front solo LEE y DIBUJA, no transforma
```

Principio rector: **las cuentas se hacen en el pipeline, no en el navegador.**

## 3. Fuentes de datos

| Fuente | Cobertura | Límites (free) | Autenticación | Licencia de datos |
|---|---|---|---|---|
| [OpenF1](https://openf1.org) | Temporadas 2023 en adelante. Telemetría, vueltas, stints, pits, posiciones, intervalos, clima, dirección de carrera | 3 req/s y 30 req/min | Ninguna | CC BY-NC-SA 4.0 |
| [Jolpica-F1](https://github.com/jolpica/jolpica-f1) (sucesor de Ergast) | Temporadas 1950 en adelante. Resultados, clasificaciones, calendarios | 4 req/s (ráfaga) y 500 req/h (sostenido). La documentación advierte que **van a bajar** | Ninguna. Exige `User-Agent` propio | CC BY-NC-SA 4.0, uso no comercial |

Notas:
- OpenF1: los datos de una sesión pasan a ser "históricos" (gratuitos) **30 minutos después de que termina la sesión**. Los datos en vivo son pagos y quedan fuera de alcance.
- Ergast está dado de baja (responde 404). No usarlo.

## 4. Catálogo de visualizaciones (productos de datos)

Prioridad MoSCoW: **M** = Must (MVP), **S** = Should, **C** = Could.

| ID | Visualización | Fuente | Granularidad | Prioridad |
|---|---|---|---|---|
| VIZ-01 | Comparación de telemetría de la vuelta más rápida entre pilotos (velocidad, acelerador, freno, marcha vs. distancia) | OpenF1 `car_data`, `laps` | Sesión (Carrera / Clasificación) | M |
| VIZ-02 | Ritmo de carrera: distribución de tiempos de vuelta por piloto | OpenF1 `laps`, `race_control` | Sesión de carrera | M |
| VIZ-03 | Estrategia de neumáticos: stints por piloto (compuesto, vuelta inicio–fin) | OpenF1 `stints` | Sesión de carrera | M |
| VIZ-04 | Evolución de posiciones vuelta a vuelta | OpenF1 `position`, `laps` | Sesión de carrera | M |
| VIZ-05 | Duración de pit stops por equipo | OpenF1 `pit` | Sesión de carrera / temporada | M |
| VIZ-06 | Historia: campeones, victorias y podios por piloto y constructor (1950→hoy) | Jolpica | Histórico | M |
| VIZ-07 | Progresión de puntos del campeonato a lo largo de la temporada | Jolpica (clasificaciones por ronda) | Temporada | S |
| VIZ-08 | Diferencia con el líder a lo largo de la carrera | OpenF1 `intervals` | Sesión de carrera | S |
| VIZ-09 | Clima vs. tiempos de vuelta (temperatura de pista, lluvia) | OpenF1 `weather`, `laps` | Sesión | S |
| VIZ-10 | Mapa del circuito coloreado por velocidad o marcha | OpenF1 `location`, `car_data` | Vuelta | C |

## 5. Requerimientos funcionales

### 5.1 Extracción

| ID | Requerimiento |
|---|---|
| RF-EXT-01 | El pipeline debe extraer de OpenF1 las sesiones de tipo **Carrera, Sprint y Clasificación** desde la temporada 2023. Las prácticas libres quedan fuera del MVP. |
| RF-EXT-02 | Por cada sesión en alcance debe extraer: `sessions`, `meetings`, `drivers`, `laps`, `stints`, `pit`, `position`, `weather`, `race_control`, `session_result` y, en carreras, `intervals`. |
| RF-EXT-03 | La telemetría (`car_data`) se debe extraer **solo para la vuelta más rápida de cada piloto**, filtrando por la ventana temporal de esa vuelta (`date>=inicio&date<=fin`). Nunca la sesión completa. |
| RF-EXT-04 | El pipeline debe extraer de Jolpica, para todas las temporadas desde 1950: temporadas, calendario, resultados de carrera y clasificaciones finales de pilotos y constructores. |
| RF-EXT-05 | Solo se deben procesar sesiones cuya hora de fin más 30 minutos sea anterior al momento de ejecución. El listado de la temporada en curso puede incluir sesiones aún no disputadas. |
| RF-EXT-06 | El cliente HTTP debe respetar los límites de cada fuente con margen de seguridad (objetivo: ≤ 80 % del límite publicado). OpenF1: ≤ 30 req/min. Jolpica: ≤ 500 req/h. |
| RF-EXT-07 | Ante HTTP 429 o 5xx, el cliente debe reintentar con backoff exponencial y un máximo de reintentos configurable. |
| RF-EXT-08 | Un HTTP 404 en un endpoint de una sesión se debe registrar como **"sin datos"**, no como error. (Verificado: `starting_grid` devolvió 404 para una carrera de 2024.) |
| RF-EXT-09 | Jolpica se debe paginar con `limit=100` (máximo permitido) y `offset` hasta cubrir el `total` informado. |
| RF-EXT-10 | Todas las peticiones a Jolpica deben enviar un `User-Agent` que identifique al proyecto. |
| RF-EXT-11 | La extracción debe ser **incremental**: el pipeline mantiene un registro de sesiones y temporadas ya procesadas y solo extrae lo nuevo. |
| RF-EXT-12 | La carga histórica inicial (backfill) debe ser **reanudable y fraccionable por temporada**, de modo que cada ejecución entre en el límite de un job de GitHub Actions (6 h). |

**Criterios de aceptación (ejemplos)**

- Dado que una carrera terminó hace 10 minutos, cuando corre el pipeline, entonces la sesión **no** se procesa y queda pendiente para la siguiente corrida.
- Dado que OpenF1 responde 429, cuando el cliente reintenta, entonces espera cada vez más entre intentos y la corrida no supera las 30 req/min.
- Dado que una sesión ya fue procesada, cuando el pipeline vuelve a correr, entonces no se vuelven a pedir sus endpoints.

### 5.2 Capa raw

| ID | Requerimiento |
|---|---|
| RF-RAW-01 | Cada respuesta se debe guardar **sin modificar**, junto con: fuente, URL, parámetros, fecha y hora de extracción (UTC) y código HTTP. |
| RF-RAW-02 | La capa raw debe permitir **reprocesar** todas las transformaciones sin volver a llamar a las APIs. |
| RF-RAW-03 | La capa raw **no** se publica en GitHub Pages. |
| RF-RAW-04 | La capa raw se versiona en el repositorio **comprimida con gzip**, con un archivo por fuente, sesión o temporada y endpoint. |
| RF-RAW-05 | Los archivos raw son **inmutables**: se escriben una sola vez y nunca se modifican. Así no inflan el historial de git. Si hace falta re-extraer, se escribe una nueva versión con su fecha de extracción. |

### 5.3 Transformación

| ID | Requerimiento |
|---|---|
| RF-TRF-01 | Todos los timestamps se deben normalizar a UTC con tipo fecha/hora. Cada tabla debe tener tipos explícitos, no inferidos. |
| RF-TRF-02 | Se deben deduplicar registros por clave natural (ej. `session_key + driver_number + lap_number` en vueltas). |
| RF-TRF-03 | Las vueltas se deben **marcar, no borrar**, con banderas: vuelta de salida de boxes (`is_pit_out_lap`), sin tiempo registrado, bajo Safety Car / VSC / bandera roja (derivado de `race_control`). VIZ-02 usa solo vueltas "limpias". |
| RF-TRF-04 | La telemetría no trae distancia: se debe **calcular la distancia recorrida** dentro de la vuelta integrando velocidad × Δt, para poder comparar pilotos por posición en pista (VIZ-01). |
| RF-TRF-05 | Las posiciones (`position`) vienen por timestamp: se deben **mapear a número de vuelta** usando el `date_start` de cada vuelta (VIZ-04). |
| RF-TRF-06 | Debe existir una **dimensión de pilotos conformada** que unifique la identidad entre OpenF1 (`driver_number`) y Jolpica (`driverId`). Los números de piloto pueden cambiar entre temporadas. |
| RF-TRF-07 | Debe existir una **dimensión de constructores conformada** que contemple los cambios de nombre de los equipos a lo largo del tiempo (ej. AlphaTauri → RB). |
| RF-TRF-08 | Los campeonatos históricos se deben tomar de las **clasificaciones oficiales** (Jolpica), no recalcular a partir de resultados: hasta 1990 existían resultados descartados y los sistemas de puntuación cambiaron varias veces. |
| RF-TRF-09 | `intervals` (~30.000 filas y ~4,2 MB por carrera) se debe **agregar por vuelta** antes de llegar a gold. |

### 5.4 Calidad de datos

| ID | Requerimiento |
|---|---|
| RF-DQ-01 | Unicidad de claves naturales en todas las tablas transformadas. |
| RF-DQ-02 | No nulos en campos críticos (claves, piloto, sesión, tiempos usados por un gráfico). |
| RF-DQ-03 | Rangos válidos: velocidad 0–400 km/h, acelerador 0–100, marcha 0–8, posición ≥ 1, duración de vuelta > 0. |
| RF-DQ-04 | Integridad referencial: toda vuelta, stint y pit stop referencia un piloto existente en la sesión. |
| RF-DQ-05 | La cantidad de pilotos por sesión debe estar dentro de lo esperado para la temporada. |
| RF-DQ-06 | Si falla un control **crítico**, la corrida termina en error y **no se publica**: el sitio sigue mostrando la última versión válida (fail closed). |
| RF-DQ-07 | El resultado de cada control se registra en el manifiesto de la corrida. |

### 5.5 Carga (capa gold) y publicación

| ID | Requerimiento |
|---|---|
| RF-GLD-01 | Cada visualización consume **su propio dataset**, ya con la forma del gráfico (agregado, filtrado y ordenado). El front no hace joins ni agregaciones. |
| RF-GLD-02 | Cada dataset gold tiene un **contrato documentado**: campos, tipos, unidades y versión de esquema (`schema_version`). |
| RF-GLD-03 | Se debe publicar un `manifest.json` con el catálogo disponible (temporadas, sesiones, pilotos, datasets y fecha de actualización) para que el front arme sus selectores. |
| RF-GLD-04 | Se debe publicar un `status.json` con la salud del pipeline: última corrida, duración, sesiones procesadas, peticiones realizadas y controles de calidad. |
| RF-GLD-05 | La publicación en GitHub Pages ocurre solo si la corrida completa terminó bien. |

### 5.6 Orquestación

| ID | Requerimiento |
|---|---|
| RF-ORQ-01 | Ejecución programada (cron) **semanal, cada lunes (UTC)**, para captar las sesiones del fin de semana anterior. |
| RF-ORQ-02 | Ejecución manual con parámetros: temporada, sesión, modo backfill y reproceso desde raw. |
| RF-ORQ-03 | No puede haber dos corridas simultáneas (grupo de concurrencia). |
| RF-ORQ-04 | El workflow programado **no se debe desactivar en el receso de temporada**. GitHub desactiva los cron de repos públicos tras 60 días sin actividad, y el receso de invierno (diciembre a marzo) puede superar ese plazo. |

## 6. Requerimientos no funcionales

| ID | Categoría | Requerimiento |
|---|---|---|
| RNF-01 | Costo | Costo operativo $0: solo planes gratuitos de las APIs, GitHub Actions y GitHub Pages. |
| RNF-02 | Tamaño | Sitio publicado muy por debajo del límite de 1 GB de Pages (objetivo ≤ 200 MB). Cada archivo gold ≤ 1 MB (objetivo de carga en el navegador). Ningún archivo versionado ≥ 50 MiB (GitHub avisa en 50 MiB y bloquea en 100 MiB). Repositorio < 1 GB. |
| RNF-03 | Tiempo | Corrida incremental < 15 min. Cada job de backfill < 6 h (límite de GitHub Actions). El deploy a Pages debe terminar en menos de 10 min (timeout de Pages). |
| RNF-04 | Reproducibilidad | Con la misma capa raw, la transformación produce exactamente la misma salida. Las dependencias tienen versiones fijadas. |
| RNF-05 | Testing | Desarrollo con **TDD estricto**. Tests unitarios de transformaciones sobre fixtures (respuestas reales grabadas), tests de contrato de los esquemas gold y tests del cliente HTTP (rate limit, reintentos, 404). Los tests unitarios **no** hacen llamadas de red. |
| RNF-06 | Seguridad | Permisos mínimos del `GITHUB_TOKEN` por job. No hace falta ningún secreto en el MVP (ambas APIs son sin key), pero la configuración debe permitir agregarlos sin tocar código. |
| RNF-07 | Extensibilidad | Agregar una nueva fuente (OpenDota, fase 2) **no** debe requerir modificar el código de F1. Cada fuente es un módulo independiente sobre una base común (cliente HTTP, raw, calidad, publicación). |
| RNF-08 | Observabilidad | Logs estructurados por corrida. Ante un fallo, el log permite identificar fuente, endpoint y sesión. |
| RNF-09 | Legal | Ver sección 7. |

## 7. Requerimientos legales y de atribución

| ID | Requerimiento |
|---|---|
| LEG-01 | Los datos publicados derivados de OpenF1 y Jolpica se licencian bajo **CC BY-NC-SA 4.0**. ShareAlike obliga a usar la misma licencia para los datos derivados. El código del proyecto puede tener una licencia distinta. |
| LEG-02 | El sitio debe atribuir a OpenF1 (con link a openf1.org) y a Jolpica-F1 como fuentes de datos. |
| LEG-03 | El sitio debe incluir un aviso visible de que es un proyecto **no oficial** y no está asociado a Formula 1 ni a la FIA. |
| LEG-04 | No se usan logos ni marcas registradas de F1 ("F1", "FORMULA 1" y relacionadas son marcas de Formula One Licensing B.V.) como identidad visual del sitio. |
| LEG-05 | Uso estrictamente **no comercial**: es un requisito de ambas licencias y de las políticas de GitHub Pages. |
| LEG-06 | El repositorio es público, así que la raw commiteada es una **redistribución** de los datos. La carpeta raw debe incluir un archivo de licencia (CC BY-NC-SA 4.0) con la atribución a OpenF1 y Jolpica-F1. |

## 8. Fuera de alcance (fase 1)

- Datos en vivo (son pagos en OpenF1).
- Sesiones de práctica libre.
- Telemetría completa de sesión: solo vuelta más rápida (RF-EXT-03).
- Radio de equipo (`team_radio`).
- OpenDota (fase 2, con su propio documento de requerimientos).

## 9. Riesgos

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Jolpica anuncia que bajará sus límites | El backfill y la corrida incremental tardan más | Límites configurables. Backfill histórico de una sola vez, luego solo incremental |
| Ambas APIs son comunitarias y no oficiales, sin garantía de disponibilidad | Corridas fallidas | Fail closed (RF-DQ-06), reintentos, raw persistida para reprocesar |
| Un endpoint cambia de esquema | Transformaciones rotas | Tests de contrato sobre las respuestas raw. El fallo se detecta antes de publicar |
| GitHub no define qué cuenta como "actividad" para la regla de 60 días | Cron desactivado en el receso | Commit del manifiesto en cada corrida (sección 10). Validar antes de diciembre que los commits del bot cuentan como actividad |
| Con cron semanal, una corrida fallida se reintenta recién a la semana siguiente | Gráficos desactualizados hasta 7 días | Disparo manual (RF-ORQ-02) tras corregir el fallo |
| OpenF1 solo cubre desde 2023 | Los gráficos de detalle no tienen historia larga | La historia larga sale de Jolpica (VIZ-06, VIZ-07) |

## 10. Decisiones de diseño

Estas decisiones definen el **cómo**:

| Decisión | Elección | Estado | Implicancias |
|---|---|---|---|
| Lenguaje del pipeline | Python | Decidido | — |
| Motor de transformación | DuckDB | Decidido | Las transformaciones se escriben en SQL. DuckDB lee el JSON crudo y exporta JSON directamente |
| Formato gold | JSON, un archivo por gráfico | Decidido | ECharts consume JSON de forma nativa. Vigilar RNF-02 (≤ 1 MB por archivo), porque JSON pesa más que Parquet |
| Capa raw | Commiteada en el repo, comprimida con gzip | Decidido | Permite reprocesar sin llamar a las APIs (RF-RAW-02). Estimación: unos 100 MB para 2023–2026 (una carrera medida: 0,38 MB). Ver RF-RAW-04, RF-RAW-05 y LEG-06 |
| Mecanismo anti-desactivación (RF-ORQ-04) | Commit del manifiesto en cada corrida | Decidido | Con el cron semanal hay al menos un commit por semana, incluso en el receso |
| Frecuencia del cron | Semanal, cada lunes (UTC) | Decidido | Cualquier horario del lunes deja pasar más de 30 min desde la última sesión del fin de semana |
| Librería de gráficos | Apache ECharts | Decidido (provisorio) | — |
| Framework del front | — | Pendiente | Se define en el documento del front |

## Anexo A — Evidencia verificada (2026-10-19)

Mediciones hechas contra las APIs reales para dimensionar los requerimientos:

| Medición | Resultado |
|---|---|
| Sesiones OpenF1 por temporada | 2023: 118 · 2024: 123 · 2025: 123 · 2026: 131 (todas las sesiones, incluidas prácticas) |
| Sesiones de carrera (incluye sprints) por temporada | 2023: 29 · 2024: 30 · 2025: 30 · 2026: 31 |
| `laps` de un piloto en una carrera (Sakhir 2024) | 57 filas, ~28 KB |
| `car_data` solo de la vuelta más rápida | 348 filas, ~58 KB (frente a ~5,9 MB de la sesión completa de un piloto) |
| `intervals` de una carrera | 29.844 filas, ~4,2 MB |
| `position` de una carrera | 698 filas, ~80 KB |
| `starting_grid` para una carrera de 2024 | HTTP 404 |
| Jolpica `limit` máximo | 100 (pedir 1000 devuelve 100) |
| Jolpica resultados 2024 | 479 filas → 5 páginas |
| Jolpica temporadas disponibles | 77 (1950–2026) |
| Raw de una carrera completa sin telemetría (9 endpoints, Sakhir 2024) | 4,71 MB sin comprimir → **0,38 MB con gzip** (~12×). `intervals` representa el 90 % del peso |

Estimación de backfill de OpenF1 (2023→2026, Carrera + Sprint + Clasificación): ~240 sesiones × ~30 peticiones ≈ 7.200 peticiones. A 30 req/min son unas **4 horas**, demasiado cerca del límite de 6 h de un job. De ahí la necesidad de fraccionar por temporada (RF-EXT-12).

## Anexo B — Glosario

| Término | Significado |
|---|---|
| Meeting | Fin de semana de Gran Premio (agrupa varias sesiones). `meeting_key` en OpenF1 |
| Session | Una sesión concreta: práctica, clasificación, sprint o carrera. `session_key` en OpenF1 |
| Stint | Tramo de carrera con un mismo juego de neumáticos |
| Raw / Silver / Gold | Capas de datos: crudo tal cual llega, limpio y modelado, listo para el gráfico |
| Fail closed | Si algo falla, no se publica nada nuevo y queda la última versión válida |
| Backfill | Carga inicial de datos históricos |
