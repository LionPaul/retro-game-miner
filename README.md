# 🎮 Multi-Console Data Mining Pipeline: From Raw Web Data to Business Intelligence

<div align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion-blue?style=for-the-badge&logo=databricks" alt="Architecture"/>
  <img src="https://img.shields.io/badge/Pipeline-Modular_ETL-orange?style=for-the-badge" alt="Pipeline"/>
  <img src="https://img.shields.io/badge/Data_Marts-Zero_Bias-green?style=for-the-badge" alt="Data Marts"/>
</div>

---

## 1. About the Project

### 1.1 Project Overview & Engineering Scope
This repository houses a production-grade, metadata-driven **Data Engineering Pipeline** designed to orchestrate the automated extraction, schema conformance, programmatic enrichment, and multi-dimensional segmentation of historical video game data. 

Engineered with an **agnostic and decoupled framework**, the entire infrastructure is abstracted via global parameters (such as `<CONSOLE_NAME>`). This allows the entire pipeline to scale horizontally across different console generations (e.g., *Super Nintendo*, *PlayStation*, *Mega Drive*) by simply altering environmental variables, requiring **zero codebase refactoring**.

The core system architecture addresses a critical flaw in community-driven cultural datasets: **high structural fragmentation, linguistic noise, and lack of relational integrity**. By building custom distributed extraction layers that crawl semi-structured web data, combined with advanced heuristics to query corporate REST APIs, this pipeline transforms chaotic data footprints into highly optimized, curated data warehouses.

### 1.2 Structural Core Features
* **Pentatendativa Lookup Protocol:** A proprietary 5-tier semantic fallback algorithm engineered to resolve text discrepancies between colloquial wiki titles and strict corporate API keys, maximizing data recovery while saving API bandwidth.
* **Granular Operational Staging:** Implements checkpoint persistence across 7 sequential stages, functioning as an immutable data lineage log for rapid debugging and trace auditing.
* **Dual-Target Data Marting:** Implements advanced statistical protection rules by separating business outputs into two unnumbered, ready-for-ingestion files, explicitly eliminating data-imputation and zero-variance biases.

### 1.3 Applied Business Value & Stakeholder Mapping
This pipeline is architected to deliver immediate analytical value across multiple data-driven disciplines:

| Target Persona | Operational Impact | Technical Deliverable |
| :--- | :--- | :--- |
| 🛠️ **Data Engineers** | Demonstrates adherence to the **Single Responsibility Principle (SRP)**, strict pipeline decoupling, and edge caching techniques to enforce API rate limits. | Clean, structured execution logs and modular notebook checkpoints. |
| 📊 **BI & Analytics Engineers** | Supplies highly refined, low-latency relational flat files, completely stripped of text artifacts, regional duplicates, and system metadata. | `.xlsx` data marts optimized for direct schema mapping in **Power BI** or **Tableau**. |
| 📈 **Market Researchers** | Enables robust exploratory data analysis (EDA) by mapping how technical software quality indicators correlate with historical macroeconomic global revenue. | Controlled, zero-bias analytical datasets ready for econometric modeling. |

---

## 2. Data Architecture & Medallion Design

### 2.1 The Medallion Architecture Blueprint (Bronze → Silver → Gold)
To guarantee structural integrity, strict lineage tracing, and computational efficiency, the pipeline implements a customized enterprise version of the **Medallion Architecture**. The system ingests data into an immutable raw state and executes progressive refinement, validation, and dimensional modeling phases before delivering high-fidelity analytics schemas.

| Architecture Layer | Technical Status | Platform Checkpoint | Data Governance & Transformation Rules |
| :---: | :--- | :---: | :--- |
| <img src="https://img.shields.io/badge/Layer-Bronze-brown?style=flat-square" alt="Bronze"/> | **Raw Staging** | `01_extract_raw` | **Immutable Ingestion Zone:** Captures the raw snapshot of the Wikipedia HTML table stream. No data pruning, row filtration, or header renaming is allowed at this stage to secure full historical provenance. |
| <img src="https://img.shields.io/badge/Layer-Silver-silver?style=flat-square" alt="Silver"/> | **Conformed Data** | `02_silver_clean` | **Data Conformance & Feature Engineering:** Enforces dynamic schema column alignment. Drops invalid regional entries and executes a high-performance regex engine to split, clean, and yield 5 strategic search variables per row. |
| <img src="https://img.shields.io/badge/Layer-Gold-gold?style=flat-square" alt="Gold"/> | **Analytical Marts** | `03` to `09` | **Enrichment & Dimensional Modeling:** Integrates external metadata through multi-fallback API protocols (Twitch/IGDB ratings, structural genre dictionaries, and global financial records). Splits the master dataframe into specialized, zero-bias analytical data marts. |

## 2. Data Architecture & Medallion Design

### 2.1 The Medallion Architecture Blueprint (Bronze → Silver → Gold)
To guarantee structural integrity, strict lineage tracing, and computational efficiency, the pipeline implements a customized enterprise version of the **Medallion Architecture**. The system ingests data into an immutable raw state and executes progressive refinement, validation, and dimensional modeling phases before delivering high-fidelity analytics schemas.

| Architecture Layer | Technical Status | Platform Checkpoint | Data Governance & Transformation Rules |
| :---: | :--- | :---: | :--- |
| <img src="https://img.shields.io/badge/Layer-Bronze-brown?style=flat-square" alt="Bronze"/> | **Raw Staging** | `01_extract_raw` | **Immutable Ingestion Zone:** Captures the raw snapshot of the Wikipedia HTML table stream. No data pruning, row filtration, or header renaming is allowed at this stage to secure full historical provenance. |
| <img src="https://img.shields.io/badge/Layer-Silver-silver?style=flat-square" alt="Silver"/> | **Conformed Data** | `02_silver_clean` | **Data Conformance & Feature Engineering:** Enforces dynamic schema column alignment. Drops invalid regional entries and executes a high-performance regex engine to split, clean, and yield 5 strategic search variables per row. |
| <img src="https://img.shields.io/badge/Layer-Gold-gold?style=flat-square" alt="Gold"/> | **Analytical Marts** | `03` to `09` | **Enrichment & Dimensional Modeling:** Integrates external metadata through multi-fallback API protocols (Twitch/IGDB ratings, structural genre dictionaries, and global financial records). Splits the master dataframe into specialized, zero-bias analytical data marts. |

### 2.2 End-to-End Data Pipeline Lineage
The flowchart below maps the linear execution path of the ecosystem, showcasing how each module operates with strict decoupling—reading exclusively from the artifact produced by the previous staging layer to respect the Single Responsibility Principle.

```text
  [ INGESTION STAGE ]
         │
         ▼
  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌────────────────┐
  │  Wikipedia   │ ───> │  01_Bronze   │ ───> │  02_Silver   │ ───> │    03_Gold     │
  │  HTML Stream │      │  (Raw HTML)  │      │ (Clean/Regex)│      │(API Populated) │
  └──────────────┘      └──────────────┘      └──────────────┘      └────────────────┘
                                                                             │
  [ PROCESSING & ENRICHMENT STAGE ]                                          ▼
  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌────────────────┐
  │  07_Market   │ <─── │   06_Gold    │ <─── │   05_Gold    │ <─── │    04_Gold     │
  │(Best-Sellers)│      │(Sales Merge) │      │ (Categories) │      │ (Filter Scored)│
  └──────────────┘      └──────────────┘      └──────────────┘      └────────────────┘
         │                                               │                    
         ▼ [ PRODUCTION DATA MARTS ]                     ▼                    
  ┌──────────────────────────────┐                    ┌──────────────────────────────┐
  │  <CONSOLE_NAME>_financial    │                    │  <CONSOLE_NAME>_technical    │
  │         _performance         │                    │         _performance         │
  │  (No-Bias Sales Data Mart)   │                    │ (No-Bias Quality Data Mart)  │
  └──────────────────────────────┘                    └──────────────────────────────┘
``````

## 3. The 9-Stage Dataset Ecosystem

The architecture is designed to map its execution footprint clearly. Throughout the process, the pipeline generates **9 distinct spreadsheet files** inside the `/datasets/` directory. Each file represents a specific operational milestone or a dedicated analytical data mart.

### 3.1 Pipeline Internal Datasets (Staging Logs 1 to 7)
These files act as immutable checkpoints for data lineage, allowing debugging at any specific point of the transformation chain without re-running previous heavy compute stages.

1. **`1_<CONSOLE_NAME>_games_raw.xlsx` (Bronze Layer)**
   * *Description:* The absolute raw snapshot of the Wikipedia HTML extraction.
   * *Key Characteristics:* Contains original multi-index headers, Wikipedia footnote brackets (`[a]`, `[1]`), regional release dates spread across multiple columns, and unreleased titles.
2. **`2_<CONSOLE_NAME>_games_cleaned.xlsx` (Silver Layer)**
   * *Description:* The standardized and linguistically expanded version of the dataset.
   * *Key Characteristics:* Irrelevant regional markets are dropped; column headers are normalized dynamically; regional release dates are collapsed into a single `Original_Release` attribute; and 5 distinct semantic lookup variations (`T1` to `T5`) are engineered per row.
3. **`3_<CONSOLE_NAME>_games_populated.xlsx` (Gold Populated Layer)**
   * *Description:* The first enriched dataset containing scores from the external API.
   * *Key Characteristics:* Features the new `IGDB_Score` column, populated by the cascading *Pentatendativa* search protocol execution log. Contains null values for non-indexed games.
4. **`4_<CONSOLE_NAME>_games_scored_only.xlsx` (Gold Filtered Layer)**
   * *Description:* The optimized, high-fidelity engineering gatekeeper dataset.
   * *Key Characteristics:* Completely cleaned of non-analyzable titles (rows with missing scores). Enforces unique primary keys via duplicate expurgation to save compute overhead on downstream processes.
5. **`5_<CONSOLE_NAME>_games_with_genres.xlsx` (Gold Category Layer)**
   * *Description:* The second API-enriched dataset mapping dimensional features.
   * *Key Characteristics:* Injects 5 independent relational columns representing nested categories fetched from the IGDB dictionary mapping (`Category_Primary` through `Category_Quinary`).
6. **`6_<CONSOLE_NAME>_games_perfect_list.xlsx` (Ultimate Gold Master Layer)**
   * *Description:* The definitive integrated repository of the console's active catalog.
   * *Key Characteristics:* Integrates historical parameters, score metrics, classifications, and global sales metrics scraped from the financial Wikipedia stream. Non-bestselling titles default to a `0` value here.
7. **`7_<CONSOLE_NAME>_games_bestsellers.xlsx` (Gold Specialized Business Layer)**
   * *Description:* A segmented subset isolating market record-breakers.
   * *Key Characteristics:* Features exclusively titles that broke the historical threshold of 1 million global copies sold, sorted in descending order by commercial performance.

### 3.2 Final Business Intelligence Artifacts (Data Marts)
These unnumbered spreadsheets are the ultimate deliverables of the data pipeline. They are specifically shaped for seamless visual rendering and human analytics, completely stripped of data pipeline overhead (such as operational search keys `T2` to `T5`).

8. **`<CONSOLE_NAME>_financial_performance.xlsx` (Sales Data Mart)**
   * *Target Analysis:* Macroeconomic and financial insights.
   * *Design Philosophy:* Inherits data exclusively from Step 7. By dropping games without verified commercial tracking, it eliminates zero-variance data-imputation biases, allowing clean statistical correlation models between quality (scores) and revenue (copies sold) among market leaders.
9. **`<CONSOLE_NAME>_technical_performance.xlsx` (Quality Data Mart)**
   * *Target Analysis:* Historical trend analysis and studio evaluation.
   * *Design Philosophy:* Inherits data from Step 5. It contains all evaluated games in history, optimized with singular clean columns for primary genres and corporate names. Perfect for boxplots of scores by genre or historical timeline charts.

---

## 4. Technical Implementation & Features

The engineering backbone of this pipeline consists of modular orchestration scripts that enforce strict governance rules on strings, data structures, and API connection budgets.

### 4.1 Automated Web Scraping & Normalization (Notebook 01 & 02)
* **HTML Table Extraction:** Uses `pandas.read_html` coupled with targeted `requests` sessions to crawl semi-structured content from historical streams.
* **Dynamic Header Resolution:** Automatically flattens multi-index rows and strips out localized clutter.
* **The Chronological Collapse:** Converts fragmented multi-regional launch data (columns such as JP, NA, PAL) into a single unified chronological anchor (`Original_Release`) using custom datetime casting rules.

### 4.2 The "Pentatendativa" Search Protocol (Notebook 03)
To overcome text discrepancies between community-written articles and strict corporate API naming conventions, the pipeline executes a cascading **5-Tier Fallback Protocol**. If a query fails to return a result from the IGDB engine, the system automatically falls back to alternative semantic signatures:

```text
  [Title Search] ──(No Match?)──> [T1: Stripped Base Title]
                                           │
  [T2: Regional Fallback] <───────(No Match?)
            │
            ▼
  [T3: Key Terms Only] ───(No Match?)──> [T4: Alphanumeric Acronym]
                                                   │
  [Final Clean Mart] <──────────(Success)──────────┘
```

* **Tier 1 (Clean Base):** Standard name parsing, eliminating symbols and trailing whitespaces.
* **Tier 2 (Fallback Reference):** Evaluates alternative localized title descriptions (e.g., Japanese or European secondary names).
* **Tier 3 (Keyword Pruning):** Extracts the core nominal sequence, stripping out technical tags, subtitles, and punctuation.
* **Tier 4 (Acronym Construction):** Condenses long multi-word titles into structural acronym keys (e.g., "The Legend of Zelda" becomes "TLOZ") to query indexing engines using abbreviations.
* **Tier 5 (Stopwords Expurgation):** Discards linguistic noise (`the`, `of`, `and`, `a`) to perform fuzzy matching on semantic roots.

### 4.3 Category Mapping & Analytical Boundary Enforcement (Notebook 04 & 05)
* **Gatekeeper Filter:** Automatically evicts records that cannot be evaluated (null `IGDB_Score`), preserving data density.
* **Computational Cost Reduction:** Runs a strict deduplication phase on lookup keys before executing deep API taxonomy requests. This prevents querying the same game multiple times, saving Twitch developer token bandwidth.
* **Dimensional Array Flattening:** Unpacks nested JSON responses from the API's genre endpoints into 5 discrete, flat relational columns (`Category_Primary` to `Category_Quinary`).

### 4.4 Financial Integration & Zero-Bias BI Layer (Notebook 06 to 09)
* **Fuzzy Left Join:** Marries technical records with historical financial data using normalized alphanumeric join-keys, ignoring casing, spaces, and punctuation anomalies.
* **Zero-Variance Imputation Remediation:** Instead of polluting the global dataset with arbitrary zeros for unrecorded sales, the pipeline decouples data deliveries:
  * **`<CONSOLE_NAME>_financial_performance.xlsx`** keeps strictly rows with commercial validation (Sales > 0), allowing realistic economic regressions.
  * **`<CONSOLE_NAME>_technical_performance.xlsx`** holds the complete tech-catalog view, optimized with localized-string sanitization (splitting multiple studio labels like `Square (JP/NA) Nintendo (PAL)` into clean primary entities like `Square`).

---
## 5. How to Run the Environment

### 5.1 Prerequisites & API Credentials
To execute the enrichment stages (Notebooks 03 and 05), you must obtain valid developer credentials from the **Twitch Developer Portal**. The pipeline automatically handles OAuth2 token generation using these variables.

1. Create a developer account at [Twitch Dev Portal](https://dev.twitch.tv/).
2. Register a new application to generate a `Client ID` and a `Client Secret`.
3. Set up your local environment variables or configure them directly within your execution environment:

```bash
export TWITCH_CLIENT_ID="your_client_id_here"
export TWITCH_CLIENT_SECRET="your_client_secret_here"
```

### 5.2 Required Libraries
The entire environment can be managed via standard package managers. Ensure you have the following dependencies installed:

```bash
pip install pandas requests openpyxl lxml
```

### 5.3 Step-by-Step Execution Sequence
The notebooks must be executed sequentially, as each step validates and updates the historical data checkpoints stored in the `/datasets/` directory:

```text
Step 01: 01_extract_raw.ipynb          --> Extracts Wikipedia HTML structure into Bronze Layer.
Step 02: 02_silver_clean.ipynb         --> Standardizes schema alignment and constructs T1-T5 keys.
Step 03: 03_api_populate.ipynb        --> Queries IGDB REST API using the Pentatendativa cascade.
Step 04: 04_filter_scored.ipynb       --> Evicts non-evaluated entries and executes primary-key deduplication.
Step 05: 05_category_mapping.ipynb   --> Flattens nested JSON genre responses into 5 relational features.
Step 06: 06_sales_merge.ipynb          --> Merges global financial metrics via fuzzy alphanumeric join-keys.
Step 07: 07_market_segmentation.ipynb --> Filters and sorts commercial hits (>1M copies sold).
Step 08: 08_financial_mart.ipynb     --> Exports cleaned, zero-bias visual analysis Data Mart.
Step 09: 09_technical_mart.ipynb     --> Exports cleaned, unified historical quality evaluation Data Mart.
```
## 6. Future Analytical Insights

The optimized outputs delivered by the pipeline (`<CONSOLE_NAME>_financial_performance.xlsx` and `<CONSOLE_NAME>_technical_performance.xlsx`) serve as highly tailored data structures ready for cross-sectional analytical modeling and dashboard ingestion (**Power BI**, **Tableau**, or **Looker Studio**).

By separating the analytical workloads, future users can confidently map metrics across two main strategic views:

### 6.1 Macroeconomic & Financial Analysis (Sales Data Mart)
* **The Revenue Curve vs. Quality:** Plotting scatter diagrams to evaluate if higher technical critical acclaim (`Score`) directly shifts market behavior and scales global demand (`Sold`).
* **Publisher Market Share:** Grouping data by the cleaned `Publisher` column to calculate total software distribution volume among market leaders, highlighting who dominated the console's commercial peak.
* **Hit-Rate Estimation:** Building predictive models to analyze what percentage of a studio's historical pipeline successfully breaks the 1 million copies threshold.

### 6.2 Ecosystem Quality Trends (Technical Data Mart)
* **Historical Timeline Evaluation:** Generating time-series line charts tracking the average `Score` over launch years (`Release`) to determine if games improved in quality as developers mastered the console's hardware technical capabilities.
* **Genre Variance (Boxplots):** Building boxplots using `Category` vs. `Score` to identify which genres had the highest consistency in production quality, and which ones suffered from highly volatile reviews.
* **Studio Performance Benchmarking:** Crossing `Developer` with `Score` metrics to find the most consistent high-performing "powerhouses" of that hardware generation, regardless of their sales records.

---
