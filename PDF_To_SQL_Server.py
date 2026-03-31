import pdfplumber
import pyodbc
import re
from pathlib import Path
from db_config import get_db_credentials

PDF_PATH = "sample.pdf"

# Build connection string
def get_connection_string() -> str:
    creds = get_db_credentials()
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};" # Make sure driver is installed
        f"SERVER={creds['server']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
    )

# Extract text and tables from PDF
def extract_pdf_data(pdf_path: str) -> list[dict]:
    records = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            tables = page.extract_tables()

            if text:
                for line in text.splitlines():
                    match = re.match(r"^(.+?):\s+(.+)$", line.strip())
                    if match:
                        records.append({
                            "page":   page_num,
                            "source": "text",
                            "key":    match.group(1).strip(),
                            "value":  match.group(2).strip(),
                        })

            for table in tables:
                headers = table[0]
                for row in table[1:]:
                    for header, cell in zip(headers, row):
                        if header and cell:
                            records.append({
                                "page":   page_num,
                                "source": "table",
                                "key":    str(header).strip(),
                                "value":  str(cell).strip(),
                            })

    return records

# Upload data to SQL Server
def upload_to_db(records: list[dict]):
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM sysobjects WHERE name='pdf_data' AND xtype='U'
        )
        CREATE TABLE pdf_data (
            id         INT IDENTITY(1,1) PRIMARY KEY,
            page       INT,
            source     NVARCHAR(50),
            key        NVARCHAR(500),
            value      NVARCHAR(MAX),
            created_at DATETIME DEFAULT GETDATE()
        )
    """)
    conn.commit()

    rows = [(r["page"], r["source"], r["key"], r["value"]) for r in records]
    cursor.executemany("""
        INSERT INTO pdf_data (page, source, key, value)
        VALUES (?, ?, ?, ?)
    """, rows)
    conn.commit()

    print(f"✅ Inserted {len(rows)} records into SQL Server")
    cursor.close()
    conn.close()

# Main execution
if __name__ == "__main__":
    pdf_file = Path(PDF_PATH)
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    print(f"Extracting data from: {PDF_PATH}")
    records = extract_pdf_data(PDF_PATH)
    print(f"   Found {len(records)} records")

    print("Uploading to SQL Server...")
    upload_to_db(records)