import pandas as pd
import mysql.connector
import numpy as np

# -----------------------------
# 1. CSV file path
csv_file = r"F:\BackOrderPredictionNew-main\BackOrderPredictionNew-main\artifacts\raw.csv"

# -----------------------------
# 2. Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",            # replace with your MySQL username
    password="Admin@123",   # replace with your MySQL password
    database="Backorder"
)
cursor = conn.cursor()

# -----------------------------
# 3. Read CSV
df = pd.read_csv(csv_file)

# -----------------------------
# 4. Convert all NaN to None for MySQL NULL
df = df.replace({np.nan: None})

# -----------------------------
# 4a. Convert Yes/No columns to 1/0 for tinyint(1)
yes_no_cols = [
    'potential_issue', 'deck_risk', 'oe_constraint', 
    'ppap_risk', 'stop_auto_buy', 'rev_stop', 'went_on_backorder'
]

for col in yes_no_cols:
    if col in df.columns:
        # Normalize text: remove spaces, lowercase
        df[col] = df[col].astype(str).str.strip().str.lower()
        df[col] = df[col].map({'yes': 1, 'no': 0, 'nan': None})

# -----------------------------
# 5. Prepare INSERT query
columns = ",".join(df.columns)
placeholders = ",".join(["%s"] * len(df.columns))
sql = f"INSERT INTO backorder_data ({columns}) VALUES ({placeholders})"

# -----------------------------
# 6. Insert all rows
for row in df.itertuples(index=False, name=None):
    clean_row = tuple(None if isinstance(x, float) and np.isnan(x) else x for x in row)
    cursor.execute(sql, clean_row)

# -----------------------------
# 7. Commit changes
conn.commit()
print(f"✅ {cursor.rowcount} rows inserted successfully.")

# -----------------------------
# 8. Close connection
cursor.close()
conn.close()
