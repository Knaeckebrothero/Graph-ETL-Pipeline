# Fessi Graph ETL Pipeline - Gemini Context

## Project Overview
This project is an **ETL pipeline** designed to construct a **Neo4j knowledge graph** (Graph-RAG) for the "Fessi" university waste disposal chatbot. It addresses the limitations of standard vector search by modeling decision logic (e.g., conditional disposal rules based on item state) rather than just retrieving text segments.

### Tech Stack
*   **Language:** Python 3.10+
*   **Database:** Neo4j (Graph DB)
*   **Infrastructure:** Podman (Container Runtime)
*   **Key Libraries:** `neo4j` (Official Driver), `python-dotenv`

## Environment Setup & Usage

### 1. Infrastructure (Neo4j)
The project uses Podman for container management.
*   **Start Neo4j:**
    ```bash
    podman-compose -f docker/podman-compose.yml up -d
    ```
    *Access Browser:* http://localhost:7474 (User: `neo4j`, Pass: `neo4j_dev`)

### 2. Python Environment
*   **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
*   **Configuration:**
    Copy `.env.example` to `.env`.
    ```bash
    cp .env.example .env
    ```

### 3. Database Management (CLI)
A utility script `src/scripts/init_db.py` is provided for common tasks:
*   **Check Connection & Stats:** `python src/scripts/init_db.py`
*   **Reset Database (Clear All):** `python src/scripts/init_db.py --reset`
*   **Verbose Mode:** `python src/scripts/init_db.py -v`

## Codebase Architecture & Conventions

### Directory Structure
*   `src/db/neo4j_db.py`: **Core Database Wrapper.** Implements a Singleton pattern (`neo4j_db` instance) for database access. Handles connection pooling and session management.
*   `src/config.py`: Handles environment variable loading via `python-dotenv`.
*   `data/`: Contains raw source files (`.xlsx`, `.csv`).

### Development Patterns
*   **Database Access:** Always use the singleton instance from `src.db.neo4j_db`.
    ```python
    from src.db import neo4j_db
    # Use context manager for sessions
    with neo4j_db.session() as session:
        session.run("...")
    ```
*   **Idempotency:** Use `MERGE` statements in Cypher queries to prevent duplicate nodes/relationships during ETL runs.
*   **Schema:** The graph follows a 3-layer ontology:
    1.  **Concept Layer:** `WasteItem`, `AVVCode`
    2.  **Rule Layer:** `DisposalRule`, `Condition`
    3.  **Infrastructure Layer:** `Container`, `Room`, `Building`

## CI/CD
*   GitHub Actions workflow (`.github/workflows/main.yml`) exists but is currently a placeholder. No strict linting/testing pipeline is currently enforced by CI.
