# Projeto de Pipeline de Dados: Ingestão e Transformação

Este projeto implementa um pipeline de dados robusto para ingerir dados brutos de um Object Storage (MinIO), processá-los e carregá-los de forma estruturada em um Data Warehouse colunar (ClickHouse). O fluxo é orquestrado pelo Prefect, garantindo confiabilidade e tratamento de erros.

## Sumário

1. [Arquitetura do Pipeline](#1-arquitetura-do-pipeline)
2. [Fluxo e Modelagem de Dados](#2-fluxo-e-modelagem-de-dados)
3. [Métricas e Controles de Qualidade](#3-métricas-e-controles-de-qualidade)
4. [Eficiência e Otimizações](#4-eficiência-e-otimizações)
5. [Análise de Execução](#5-análise-de-execução)
6. [Tecnologias Utilizadas](#6-tecnologias-utilizadas)
7. [Guia de Execução](#7-guia-de-execução)


## 1\. Arquitetura do Pipeline

O pipeline foi desenhado seguindo a arquitetura Medalhão (Bronze/Silver), separando claramente os dados brutos dos dados tratados e prontos para consumo. A orquestração é centralizada pelo Prefect, que gerencia a execução e as dependências entre as tarefas de ingestão e transformação.

#### Diagrama de Fluxo

![Diagrama de Fluxo](./images/diagrama%20de%20fluxo.png)

#### Componentes

  * **MinIO (Landing Zone):** Atua como o repositório central para os arquivos CSV brutos, nossa camada de aterrissagem.
  * **Ingestion Script (`ingestor`):** Responsável por ler os arquivos do MinIO, realizar uma conversão mínima para o formato JSON e carregar os dados na tabela Bronze do ClickHouse.
  * **Transformation Script (`transformer`):** Lê os dados da camada Bronze, aplica regras de negócio, valida o schema com Pydantic, enriquece os dados e os carrega na tabela Silver, garantindo idempotência.
  * **ClickHouse (Data Warehouse):** Armazena os dados em duas camadas: uma tabela Bronze para dados brutos e uma tabela Silver para dados limpos e otimizados.
  * **Prefect (Orquestrador):** Gerencia o fluxo de trabalho, disparando as tarefas de ingestão e transformação em sequência e tratando falhas.


## 2\. Fluxo e Modelagem de Dados

#### 2.1. Da Fonte ao Object Storage

  * **Fonte:** Arquivos em formato **CSV**.
  * **Organização:** Para garantir a escalabilidade, os arquivos no bucket `ingestion` do MinIO deveriam seguir uma estrutura de partição por data, como `ingestion/bronze/tracks/YYYY/MM/DD/arquivo.csv`. Isso permite reprocessamentos e ingestões incrementais de forma eficiente.

#### 2.2. Camadas no Data Warehouse (ClickHouse)

  * **Bronze (`default.data_ingestion`):** Esta tabela armazena os dados brutos, com cada linha do CSV original convertida em um único campo JSON (`data_value`).

      * **Justificativa:** Manter uma camada de staging com schema flexível (JSON) acelera a ingestão e permite que a validação e transformação ocorram posteriormente, sem risco de falhas por inconsistência de schema na origem.

  * **Silver (`default.working_g2`):** A tabela de destino, contendo os dados após o tratamento.

      * **Fluxo de Transformação:** Cada JSON da camada Bronze é validado contra um modelo Pydantic (`Track`). Se válido, é enriquecido com campos derivados (`decade`, `age_years`, `tempo_bucket`, etc.). Se inválido, o erro é registrado no próprio JSON para rastreabilidade.
      * **Justificativa:** Manter o formato JSON denormalizado nesta camada preserva a flexibilidade e a rastreabilidade completa do dado, enquanto o enriquece e valida. Esta tabela serve de base para a camada analítica.

  * **Gold (Proposta para Análise):**

      * **Descrição:** Embora não implementada, a próxima etapa seria criar uma camada "Gold" a partir da tabela Silver. Isso envolveria a criação de Views ou Tabelas Materializadas no ClickHouse para projetar os dados em um modelo dimensional (Fato/Dimensões), como `Dim_Artist`, `Dim_Album` e `Fact_Track`.
      * **Justificativa:** Consultas analíticas e de BI performam melhor em modelos colunarizados com tipos de dados explícitos e chaves bem definidas, o que um modelo dimensional oferece.

## 3\. Métricas e Controles de Qualidade

Para monitorar a saúde e a qualidade do pipeline, foi implementado um sistema de coleta de métricas em tempo de execução.

  * **Armazenamento:** As métricas de cada lote processado são salvas na tabela `default.pipeline_metrics` no ClickHouse, permitindo análises históricas e a criação de dashboards.

  * **Métricas Coletadas:**

      * **Volume de Dados:** `rows_processed`, `rows_valid`, `rows_invalid`.
      * **Qualidade dos Dados:** `duplicate_rows`, `null_counts` (um mapa contando nulos por coluna).
      * **Saúde do Pipeline:** `execution_timestamp`, `status` (`SUCCESS`/`FAILURE`), `status_message`.

  * **Controles de Qualidade:**

      * **Consistência de Schema:** A validação de cada registro contra o modelo Pydantic `Track` garante que todos os dados na camada Silver estejam em conformidade com o schema esperado.
      * **Regra de Negócio:** O pipeline está configurado para falhar se mais de 10% das linhas de um lote forem inválidas, evitando a propagação de dados de baixa qualidade.


## 4\. Eficiência e Otimizações

Para garantir um pipeline escalável e confiável, foram implementadas as seguintes otimizações:

  * **Idempotência:** A tabela Silver (`default.working_g2`) utiliza o motor `ReplacingMergeTree` do ClickHouse. Este motor remove automaticamente registros duplicados com base em uma chave de ordenação (`ORDER BY`). Isso garante que múltiplas execuções do pipeline com os mesmos dados não gerem duplicatas no destino.

  * **Particionamento:** Para otimizar a performance de consultas, a tabela Silver é particionada por mês (`PARTITION BY toYYYYMM(...)`). Isso permite que o ClickHouse ignore partições irrelevantes em consultas que filtram por data, resultando em uma leitura de dados muito mais rápida e eficiente.

  * **Tratamento de Erros:** O orquestrador Prefect é configurado para gerenciar falhas. Se uma tarefa falhar (ex: por uma regra de qualidade de dados), o fluxo é interrompido, e o erro é registrado nos logs do Prefect, facilitando a depuração.

## 5\. Análise de Execução

Após a execução do pipeline com um lote de 4.500 registros, a análise dos resultados foi a seguinte:

![Tabela de métricas](./images/Tabela%20de%20métricas.png)

  * **Desempenho:** O lote de 4.500 registros foi processado em [informar tempo aproximado, ex: \~15 segundos]. A maior parte do tempo foi gasta nas operações de transformação e serialização JSON, indicando que o gargalo é computacional (CPU) e não de I/O (leitura/escrita).

  * **Qualidade dos Dados Ingeridos:** A análise da tabela `pipeline_metrics` revelou:

      * **100% de Validade:** Todos os 4.500 registros foram validados com sucesso pelo schema Pydantic (`rows_invalid = 0`).
      * **Ausência de Duplicatas e Nulos:** Não foram detectados registros duplicados ou valores nulos nos campos-chave, como indicado pelas colunas `duplicate_rows` e `null_counts`.
      * **Conclusão:** A qualidade dos dados de origem para este lote foi excelente, não exigindo intervenção ou limpeza.

  * **Sugestões de Melhorias:**

    1.  **Dashboard de Monitoramento:** Implementar um dashboard no Grafana conectado à tabela `pipeline_metrics` para visualização em tempo real da saúde do pipeline.
    2.  **Alertas:** Configurar alertas (via Grafana ou Prefect) para notificar a equipe de dados imediatamente em caso de falhas (`status = 'FAILURE'`).
    3.  **Implementar a Camada Gold:** Desenvolver as Views Materializadas para criar o modelo dimensional, otimizando o pipeline para consultas analíticas.

## 6\. Tecnologias Utilizadas

A escolha das tecnologias visou criar uma arquitetura moderna, escalável e de código aberto.

  * **Python:** Linguagem principal devido à sua vasta biblioteca para engenharia de dados (Pandas) e simplicidade.
  * **Docker:** Garante um ambiente de execução consistente, isolado e facilmente reproduzível.
  * **ClickHouse:** Selecionado como Data Warehouse por sua performance extrema em consultas analíticas (OLAP).
  * **Prefect:** Adotado como orquestrador por sua abordagem "Python-nativa", UI moderna e robusto tratamento de falhas.
  * **MinIO:** Serve como Object Storage compatível com a API S3, um padrão de mercado.

## 7\. Guia de Execução

### 7.1. Pré-requisitos

* Docker + Docker Compose instalados.
* Porta **8123** (ClickHouse HTTP), **9001** (MinIO Console) e **9002** (MinIO S3) livres no host.
* Estrutura de pastas:

  * `./ingestion/script.py` (código de ingestão)
  * `./transform/transform.py` e `./transform/schemas.py` (código de transformação)
  * `./clickhouse/init` (opcional, DDLs iniciais) e `./clickhouse/config.d` (configs extras)
  * Arquivo `docker-compose.yml` (fornecido)
  * **Arquivos CSV** para subir no MinIO (bucket `ingestion`).

> **Importante**: certifique-se de que **`./transform/__init__.py`** exista (pode estar vazio) para o pacote `transform` funcionar com `python -m transform.transform`. O módulo `schemas.py` deve estar no **mesmo pacote** (import relativo `from .schemas import Track`).

### 7.2. Subir a stack

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