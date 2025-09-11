# Ponderada Ingestão e Tratamento de Dados SEM 4

### 0.1. Pré-requisitos

* Docker + Docker Compose instalados.
* Porta **8123** (ClickHouse HTTP), **9001** (MinIO Console) e **9002** (MinIO S3) livres no host.
* Estrutura de pastas:

  * `./ingestion/script.py` (código de ingestão)
  * `./transform/transform.py` e `./transform/schemas.py` (código de transformação)
  * `./clickhouse/init` (opcional, DDLs iniciais) e `./clickhouse/config.d` (configs extras)
  * Arquivo `docker-compose.yml` (fornecido)
  * **Arquivos CSV** para subir no MinIO (bucket `ingestion`).

> **Importante**: certifique-se de que **`./transform/__init__.py`** exista (pode estar vazio) para o pacote `transform` funcionar com `python -m transform.transform`. O módulo `schemas.py` deve estar no **mesmo pacote** (import relativo `from .schemas import Track`).

### 0.2. Subir a stack

```bash
docker compose down -v --remove-orphans

docker compose up -d --build

docker compose ps
docker logs -f clickhouse   
docker logs -f minio        
```

### 0.3. Acessar o MinIO Console e criar/upload de arquivos

* Abra **[http://localhost:9001](http://localhost:9001)** → login: `minio` / `minio12345`.
* Confirme se o bucket **`ingestion`** existe (o serviço `mc-init` cria). Se não existir, crie manualmente.
* Faça **upload** de arquivos **`.csv`** para o bucket `ingestion` (qualquer pasta/raiz). O ingestor só lê `.csv` nesse código atual.

### 0.4. Disparar ingestão e transformação

* O **ingestor** roda automaticamente ao subir a stack; ele:

  * aguarda ClickHouse, cria a tabela `default.data_ingestion` (staging/bronze) e carrega cada CSV como linhas JSON.
* O **transformer** roda automaticamente depois; ele:

  * lê `SRC_TABLE` (`default.data_ingestion`) e grava em `DST_TABLE` (`default.working_g2`).

> Se você fizer upload de **novos CSVs** depois da stack subir, reexecute:

```bash
docker restart ingestor transformer
```

### 0.5. Verificações rápidas (host)

```bash
curl -s http://localhost:8123/ping

docker exec -it clickhouse clickhouse-client -q "SHOW TABLES FROM default"
```

---

## 1) Compreensão dos dados e do fluxo de ingestão

### 1.1. Estrutura e formatos no Object Storage (MinIO)

* **Bucket**: `ingestion`
* **Formato esperado**: **CSV** (atual). JSON e Parquet não são ingeridos por padrão (ver melhorias).
* **Sugerido (organização)**:

  * `ingestion/bronze/<fonte>/<yyyy>/<mm>/<dd>/<arquivo>.csv`
  * Ex.: `ingestion/bronze/tracks/2025/09/10/tracks_batch1.csv`

> Benefícios: versionamento lógico por data/fonte, reproducibilidade, auditoria e controle de qualidade.

### 1.2. Pipeline de ingestão (MinIO ➜ ClickHouse)

* Le o **CSV** com Pandas → converte `NaN` para `None` → serializa cada linha como **JSON compacto** (`data_value`) com **tag** `data_tag` = `<object_name>-<unix>`.
* Grava na tabela **`default.data_ingestion`**:

  * `timestamp_unix UInt64` (momento da carga)
  * `data_value String` (JSON linha)
  * `data_tag String` (identificador do lote)
* **Justificativa**: manter staging **raw/sem schema rígido** projeta flexibilidade, acelera ingestões heterogêneas e permite validação/limpeza posterior.

### 1.3. Transformação (staging ➜ silver)

* A função `enrich_json` valida cada JSON contra o **modelo Pydantic `Track`** e enriquece com campos derivados: `duration_min`, `decade`, `age_years`, `tempo_bucket`, `energy_bucket`, `is_english`, `is_spanish`, `label_group`, `region`, `is_long_track`, `is_popular`, `is_high_energy` e `status_validacao`.
* Em caso de erro de validação, escreve `{ "_raw": <json>, "status_validacao": "erro", "erro": <str> }` para rastreabilidade.
* Grava na **`default.working_g2`** com a mesma estrutura de colunas (timestamp/data/tag), porém com **JSON enriquecido**.

> **Decisão de projeto**: manter JSON **denormalizado** em Silver para preservar flexibilidade e rastreabilidade, preparando projeções tabulares (dimensões/fato) via **Views/Materialized Views**.

---

## 2) Modelagem no Data Warehouse

### 2.1. Camadas

* **Staging/Bronze**: `default.data_ingestion` (raw, JSON por linha, sem quebra de schema).
* **Silver**: `default.working_g2` (JSON validado + campos derivados + status de validação).
* **Gold/Dimensional - Caso fossemos utilizar para uma consulta analítica**:

  * **Dim\_Artist(artist\_id, artist, country, region)**
  * **Dim\_Album(album\_id, album, artist\_id, label, label\_group)**
  * **Dim\_Date(date\_id, year, decade)**
  * **Fact\_Track(track\_id, album\_id, date\_id, duration\_sec, danceability, energy, popularity, tempo\_bpm, buckets/flags)**

**Justificativa**:

* Consultas analíticas, BI e métricas de qualidade performam melhor em **tabelas colunarizadas com tipos explícitos**.
* Dimensões estáveis (Artista/Álbum/Data) reduzem redundância e dão **chaves substitutas** para fatos.