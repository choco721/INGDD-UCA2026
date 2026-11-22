# INGDD-UCA2026 — Grupo 03 · Pipeline de datos VATSIM

**Ingeniería de Datos · LCD · UCA Rosario · 2026**

**Chocobares Juan Cruz · Formenti Agustín · Ortiz Victoria · Morenico Andrés**

Pipeline que ingesta todos los días el tráfico de la red de vuelo simulado
VATSIM, lo transforma con dbt en capas bronze → silver → gold, y lo publica en
PostgreSQL para un tablero de Metabase. Lo orquesta Airflow en el servidor de
la cátedra, y cada cambio pasa por un CI en GitHub Actions.

**→ La documentación completa está en [README_GRUPO.md](README_GRUPO.md).**

| Entrega | Documento |
|---|---|
| E1 · Ingesta idempotente | [docs/entregas/E1_ingesta.md](docs/entregas/E1_ingesta.md) |
| E2 · dbt y calidad | [docs/entregas/E2_dbt.md](docs/entregas/E2_dbt.md) |
| E3 · Airflow y CI | [docs/entregas/E3_orquestacion_ci.md](docs/entregas/E3_orquestacion_ci.md) |
| E4 · Serving, features, governance | [docs/entregas/E4_serving_features_governance.md](docs/entregas/E4_serving_features_governance.md) |
| Defensa | [docs/entregas/Defensa.md](docs/entregas/Defensa.md) |

El código del grupo está en `dags/`. Las carpetas `dbt/`, `scripts/`, `docker/`
de la raíz y `docker-compose.yml` son el stack de ejemplo de la cátedra.
