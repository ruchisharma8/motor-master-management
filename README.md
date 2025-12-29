# Motor Master Data Management Portal

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://motor-master-management.streamlit.app/)
[![Database](https://img.shields.io/badge/Database-Neon_Serverless_Postgres-00E599?style=flat&logo=postgresql)](https://neon.tech)

**Live Demo:** [https://motor-master-management.streamlit.app/](https://motor-master-management.streamlit.app/)  
**Repository:** [https://github.com/ruchisharma8/motor-master-management](https://github.com/ruchisharma8/motor-master-management)

---

## 📖 About the Project

A secure, database-driven web application built with **Python Streamlit** to manage vehicle master data. This portal serves as a centralized "Source of Truth" for:
* **2W & 4W Vehicle Data** (Make, Model, Variant, Technical Specs)
* **RTO Locations** (Regional Transport Offices & Codes)
* **Pincode Mapping** (Location hierarchy & Insurer logic)

The application connects to a **Neon (Serverless PostgreSQL)** cloud database, ensuring real-time data persistence and scalability.

## 📂 Project Structure

* **`app.py`**: The main application code containing the UI, authentication, and database logic.
* **`requirements.txt`**: List of Python dependencies (Streamlit, Pandas, Psycopg2, etc.).
* **`schema.sql`**: SQL commands used to initialize the tables (`mmv_master`, `rto_master`, `pincode_master`).
* **`import_*.py`**: Utility scripts for bulk migrating data from CSVs to the Neon database.
* **`.streamlit/secrets.toml`**: (Ignored by Git) Contains sensitive database credentials.

---

## 🛠️ Tech Stack

* **Frontend:** [Streamlit](https://streamlit.io/) (Python)
* **Database:** [Neon](https://neon.tech/) (Serverless Postgres)
* **Language:** Python 3.9+
* **IDE:** VS Code

---

## ⚙️ Local Development Setup

To run this application on your local machine:

**1. Clone the Repository**
```bash
git clone [https://github.com/ruchisharma8/motor-master-management.git](https://github.com/ruchisharma8/motor-master-management.git)
cd motor-master-management
