# RocketLab 2026.2 | Engenharia de Dados

Repositório com as atividades da trilha de Engenharia de Dados do Rocket Lab, desenvolvidas no Databricks com PySpark e SQL.

## Estrutura

```
.
├── atividade1/
│   ├── dados/atividade 1/
│   │   ├── fut_players_data.csv
│   │   ├── metal_bands.csv
│   │   └── pokemon_data.csv
│   └── RL_Dados_2026_2_Atividade_PySpark_Sylvio_Helt.ipynb
├── atividade2/
│   ├── 01_bronze.ipynb
│   ├── 02_silver.ipynb
│   └── 03_gold.ipynb
├── atividade_3/
│   ├── notebooks/
│   │   ├── Landing_to_Bronze.ipynb
│   │   ├── Bronze_to_Silver.ipynb
│   │   └── Silver_to_Gold.ipynb
│   ├── job.yaml
│   └── job_execucao_sucesso.png
└── README.md
```

## Tecnologias

- Databricks (Free Edition, Unity Catalog, Volumes, Workflows)
- PySpark e Spark SQL
- Delta Lake
- API PTAX do Banco Central do Brasil

---

## Atividade 1 | Introdução ao PySpark

Exercícios introdutórios de manipulação de dados com PySpark, utilizando três bases: bandas de metal, jogadores do FIFA Ultimate Team e Pokémons.

### Conteúdos abordados

- **Junção de DataFrames:** joins `inner`, `left`, `right` e `outer`, além de concatenação com `union()`
- **Alteração de DataFrames:** criação de colunas derivadas com `withColumn()` e alteração condicional com `when()` / `otherwise()`
- **Operações em grupo:** agregações com `groupBy()` e `agg()` (`sum`, `mean`, `count`)
- **Window functions:** ranqueamento por grupo com `row_number()` e `Window.partitionBy()`

### Exercícios

| Exercício | Descrição |
|---|---|
| 1.1 | Leitura da base `fut_players_data` |
| 1.2 | Jogadores "The Bests" (drible e chute acima de 90) com join para obter a nacionalidade |
| 2 | Overall médio por nacionalidade, com destaque para o melhor país e para o Brasil |
| 2.1 | Classificação dos jogadores por faixa de overall (Amador, Ruim, Ok, Bom, Ótimo, Lenda) |
| Desafio | Dream Team do Brasil na formação 4-4-2, com o maior overall por grupo de posição |
| Desafio Bônus | Dream Team sem jogadores repetidos, considerando apenas a carta de maior overall de cada atleta |

---

## Atividade 2 | Arquitetura Medalhão (CineData Analytics)

Estruturação de um catálogo de filmes (base combinada TMDB/IMDb), entregue de forma intencionalmente suja e fragmentada em 5 arquivos CSV, até uma modelagem dimensional pronta para consumo analítico.

### Camadas

| Camada | Responsabilidade |
|---|---|
| **Landing** | Volume `workspace.inputs.cinedata` com os CSVs originais |
| **Bronze** | Ingestão dos CSVs e da cotação do dólar (API PTAX) em Delta, modo append, com a coluna `ingestion_datetime`. Todas as colunas lidas como string, sem alteração de conteúdo |
| **Silver** | Limpeza, tipagem, padronização dos nomes em português e deduplicação. Recalculada a cada execução a partir do lote mais recente da Bronze |
| **Gold** | Star Schema com tabela fato, dimensões e tabelas-ponte, além das consultas de negócio |

### Tabelas

- **Bronze:** `tb_movies_info`, `tb_movies_financials`, `tb_movies_metrics`, `tb_credits_and_tags`, `tb_movies_reviews`, `tb_cotacao_dolar`
- **Silver:** `tb_info_filmes`, `tb_financeiro_filmes`, `tb_metricas_engajamento`, `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas`, `tb_cotacao_dolar`
- **Gold:** `fact_movies_performance`, `dim_movies`, `dim_genres`, `dim_people`, `dim_companies`, `dim_reviews`, `bridge_movie_genre`, `bridge_movie_person`, `bridge_movie_company`

### Principais decisões técnicas

- **Bronze sem inferência de tipos:** preserva o dado bruto. Toda conversão de tipo ocorre na Silver.
- **Leitura do último lote:** a Silver considera apenas a carga mais recente de cada tabela Bronze, o que permite reexecuções sem duplicar dados.
- **Conversão segura de tipos:** valores deslocados pelo column shift são convertidos para NULL sem interromper o pipeline.
- **Datas multiformato:** suporte a `yyyy-MM-dd`, `dd/MM/yyyy` e `MM-dd-yyyy`.
- **Valores monetários:** remoção de símbolos de moeda e expansão de abreviações (`34.0M` para 34.000.000).
- **Consolidação por filme:** financeiro e métricas com uma linha por filme, garantindo o grão da tabela fato.
- **Cotação contínua:** calendário diário com forward fill para fins de semana e feriados.
- **Lucro:** calculado apenas quando orçamento e receita estão presentes. A margem só é calculada com receita positiva.
- **Surrogate keys:** geradas com `row_number()` ordenado pela chave natural, de forma determinística entre execuções.
- **Tabelas-ponte:** resolvem as relações N:N entre filmes e gêneros, pessoas e produtoras sem duplicar a tabela fato.

### Perguntas de negócio respondidas

1. Receita total (R$) de todos os filmes
2. Top 5 filmes por popularidade
3. Quantidade de filmes por gênero
4. Top 10 filmes por receita, com `RANK()`
5. Ator com mais participações nos últimos 2 anos
6. Produtora com maior lucro nos últimos 5 anos

---

## Atividade 3 | Pipeline end-to-end com orquestração

Evolução da Atividade 2 para um pipeline orquestrado, simulando uma rotina de produção.

### Databricks Workflow

| Tarefa | Notebook | Depende de |
|---|---|---|
| `to_Bronze` | `Landing_to_Bronze` | - |
| `to_Silver` | `Bronze_to_Silver` | `to_Bronze` |
| `to_Gold` | `Silver_to_Gold` | `to_Silver` |

- **Dependências:** cada tarefa só inicia após o sucesso da anterior (`All succeeded`).
- **Agendamento:** execução diária às 14h (America/Sao_Paulo), após a publicação da PTAX do dia pelo Banco Central.
- **Compute:** Serverless.

### Entregáveis

- Notebooks em `atividade_3/notebooks/`
- Definição do Job exportada em `atividade_3/job.yaml`
- Evidência de execução com sucesso em `atividade_3/job_execucao_sucesso.png`

---

## Como executar

1. **Atividade 1:** carregar os CSVs de `atividade1/dados/atividade 1/` como tabelas no schema `workspace.default` e executar o notebook.
2. **Atividades 2 e 3:** criar o Volume `workspace.inputs.cinedata` e carregar os 5 CSVs da pasta Inputs.
3. **Atividade 2:** executar os notebooks na ordem Bronze, Silver e Gold.
4. **Atividade 3:** importar o `job.yaml` no Databricks ou executar os notebooks na ordem `Landing_to_Bronze`, `Bronze_to_Silver` e `Silver_to_Gold`.