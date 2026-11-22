# Defensa del proyecto — Plan de demostración

**Grupo 03 · Ingeniería de Datos · LCD · UCA Rosario · 2026**

Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés

Repositorio: https://github.com/choco721/INGDD-UCA2026

---

## Qué está desplegado en el servidor de la cátedra

| Componente | Dónde | Estado |
|---|---|---|
| Ingesta | `dags/scripts/ingesta.py`, tarea `ingerir_bronze` | Diaria, 18:00 |
| Bronze | `s3://grupo03/bronze/vatsim` (Delta, MinIO) | Desde el 22/10/2026 |
| Transformación | `dags/dbt/`: 8 modelos, 33 tests | Tareas `dbt_run` y `dbt_test` |
| Orquestación | DAG `pipeline_grupo03` en Airflow | 5 tareas, 2 reintentos |
| Serving | PostgreSQL `warehouse.grupo03`: 6 tablas | Tarea `publicar_gold` |
| Tablero | Metabase, colección *Grupo03 · VATSIM* | 7 gráficos |
| CI | GitHub Actions, `main` protegida | En cada push |

El despliegue se hace desde el repositorio con `herramientas/desplegar.py`, que
copia la versión del repo al servidor y corre un chequeo previo. Antes de la
clase: desplegar, confirmar que el DAG corrió al menos una vez en el servidor
con esta versión, rehacer el tablero y ensayar la demo con reloj.

## Los diez minutos

Cuatro cosas, en el orden que prueba que el sistema es lo que decimos.

| Min. | Qué mostramos | Quién | Dónde |
|---|---|---|---|
| 0–1 | **Que corre.** El historial del DAG: corridas diarias, una por tarea. La frescura en el tablero: menos de 24 h. | Victoria | Airflow, Grid |
| 1–3 | **Que se reejecuta sin duplicar.** Ingesta dos veces seguidas: la primera trae la foto, la segunda dice "Sin snapshot nuevo" y escribe 0 filas. | Juan Cruz | Jupyter |
| 3–5 | **Que se recupera el pasado.** Backfill en vivo de un día elegido en el momento: la ingesta no trae nada (la fuente no guarda historia), dbt reprocesa ese día desde bronze. Mostramos los días que recibió dbt en el log. | Victoria | Airflow |
| 5–7 | **Que se puede auditar.** Tests en verde; un test fallando ante un dato malo (latitud 123,4: la gold no se construye); el linaje en `dbt docs`; el historial de versiones de bronze en Delta. | Agustín | Jupyter |
| 7–9 | **Que sirve para algo.** El tablero, gráfico por gráfico, con la pregunta que responde cada uno. | Andrés | Metabase |
| 9–10 | **Que un cambio malo no entra.** Un pull request con el CI en rojo, bloqueado. | Juan Cruz | GitHub |

Cada integrante puede explicar cualquier parte. El reparto es para la demo,
no para las preguntas.

## Cierre

### Qué haríamos distinto

1. **Editar en el repositorio desde el primer día, y desplegar con un script.**
   Editamos la carpeta del servidor a mano desde Jupyter y eso rompió el
   pipeline dos veces: archivos de respaldo del editor que dbt leyó como
   modelos, y archivos con permisos que Airflow no podía sobrescribir. El
   pipeline estuvo cinco días sin actualizar la gold.
2. **Pensar el backfill en E1.** La ingesta no recibía el intervalo de la
   corrida: lo agregamos en E3. Con una fuente sin historia, la pregunta
   "¿qué significa reprocesar el pasado?" había que hacérsela al diseñar
   bronze.
3. **Clasificar los datos personales al diseñar gold, no al final.** Durante
   dos semanas publicamos en PostgreSQL el nombre y el número de socio de cada
   piloto. El tablero nunca los usó.
4. **Medir la frescura desde el principio.** Los tests miden si el dato es
   correcto, no si llegó. Nos dimos cuenta de la caída de cinco días mirando
   Airflow, no porque algo avisara.
5. **Commits chicos y de todos.** El historial de Git no refleja cómo
   repartimos el trabajo.

### Qué quedó pendiente

- **Alertas activas.** Hoy la frescura se ve en el tablero, pero nadie recibe un
  aviso cuando el pipeline falla.
- **Un control de volumen.** Si VATSIM devolviera la mitad de los pilotos sin
  error, ningún test lo detectaría.
- **Más de una foto por día.** "Por día" hoy significa "a las 18:00". Para medir
  el día habría que muestrear cada hora.
- **La regla para la anomalía de Cabo Verde** (aviones quietos con plan de
  vuelo): necesita una serie más larga para distinguir un evento de pilotos
  ausentes.
- **Particionar bronze por fecha.** Leer el watermark recorre la columna entera;
  con el volumen actual no se nota, con años sí.
- **Las dependencias en la imagen de Airflow**, no en un entorno virtual dentro
  de la carpeta montada. Fue la solución que permitía el servidor compartido,
  no la correcta.

### Nuestro punto más débil

La fuente no guarda historia. Cada día que el pipeline no ingiere es un día
perdido para siempre, y una foto diaria es una muestra, no el total. Lo
mitigamos con reintentos, con el panel de frescura y documentando cada hueco
en la governance. No se puede resolver del todo sin cambiar de fuente.
