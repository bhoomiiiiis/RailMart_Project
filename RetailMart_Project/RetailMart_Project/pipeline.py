"""
RetailMart Data Pipeline
========================
Author  : Data Engineering Intern
Project : RetailMart Sales Data Pipeline
Purpose : Ingest raw CSV data → Clean → Transform → Load SQLite → Report

This pipeline reads three CSV files (sales, products, stores), cleans and
merges them, loads them into a SQLite database, runs SQL analytics, and
prints a business summary report.
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
import os
import sys
from datetime import datetime

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
# Logging lets us track what the program is doing step by step.
# Think of it as a diary the program writes while it runs.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),           # Print to terminal
        logging.FileHandler("pipeline.log", mode="w") # Also save to a file
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONSTANTS  (things that never change)
# ─────────────────────────────────────────────
DATA_DIR   = "data"
OUTPUT_DIR = "output"
DB_PATH    = "retail_sales.db"
TABLE_NAME = "retail_sales"

SALES_FILE    = os.path.join(DATA_DIR, "sales_data.csv")
PRODUCTS_FILE = os.path.join(DATA_DIR, "products.csv")
STORES_FILE   = os.path.join(DATA_DIR, "stores.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ══════════════════════════════════════════════
# PHASE 1 – DATA INGESTION
# ══════════════════════════════════════════════

def load_csv(filepath: str, label: str) -> pd.DataFrame:
    """
    Load a single CSV file into a pandas DataFrame.

    Args:
        filepath : Path to the CSV file.
        label    : Human-readable name for logging.

    Returns:
        A pandas DataFrame with the CSV content.

    Raises:
        FileNotFoundError : If the file does not exist.
        ValueError        : If the file is empty.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[{label}] File not found → {filepath}")

    df = pd.read_csv(filepath)

    if df.empty:
        raise ValueError(f"[{label}] File loaded but contains NO data → {filepath}")

    logger.info(f"Loaded '{label}': {df.shape[0]} rows × {df.shape[1]} cols")
    print(f"\n{'─'*50}")
    print(f"  {label.upper()} — shape: {df.shape}")
    print(f"{'─'*50}")
    print(df.head())
    return df


def check_missing_values(df: pd.DataFrame, label: str) -> None:
    """Print a clear summary of which columns have NULL values and how many."""
    missing = df.isnull().sum()
    missing = missing[missing > 0]           # keep only columns that HAVE nulls
    print(f"\n  [{label}] Missing value summary:")
    if missing.empty:
        print("    → No missing values found.")
    else:
        for col, count in missing.items():
            pct = round(count / len(df) * 100, 1)
            print(f"    • {col}: {count} missing ({pct}%)")


# Task 1 — Load all three files
def ingest_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Task 1: Load sales_data, products, and stores CSV files.
    Prints shape, first 5 rows, and missing-value summary for each.
    """
    logger.info("═══ TASK 1: Data Ingestion ═══")

    sales_df    = load_csv(SALES_FILE,    "sales_data")
    products_df = load_csv(PRODUCTS_FILE, "products")
    stores_df   = load_csv(STORES_FILE,   "stores")

    # Task 1.2 — Missing value check for all three
    print("\n  ── Missing Value Report ──")
    check_missing_values(sales_df,    "sales_data")
    check_missing_values(products_df, "products")
    check_missing_values(stores_df,   "stores")

    return sales_df, products_df, stores_df


# ══════════════════════════════════════════════
# PHASE 2 – DATA CLEANING
# ══════════════════════════════════════════════

def clean_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 2: Clean the sales DataFrame.
        Step 1 → Remove duplicate rows.
        Step 2 → Fill missing 'quantity' with 0, drop rows where 'amount' is NULL.
        Step 3 → Convert 'sale_date' to datetime, 'amount' to float.

    Args:
        sales_df : Raw sales DataFrame.

    Returns:
        Cleaned sales DataFrame.
    """
    logger.info("═══ TASK 2: Data Cleaning ═══")

    # ── Task 2.3 — Remove Duplicates ──────────────────────────────────────
    # A duplicate row is an exact copy of another row.
    # Keeping duplicates would double-count revenue — a big business error!
    before_dedup = len(sales_df)
    sales_df = sales_df.drop_duplicates()
    after_dedup  = len(sales_df)
    duplicates_removed = before_dedup - after_dedup

    print(f"\n  [Cleaning] Duplicates found and removed: {duplicates_removed}")
    logger.info(f"Duplicates removed: {duplicates_removed}")

    # ── Task 2.4 — Handle Missing Values ──────────────────────────────────
    # quantity = 0 means the sale record exists but no items were scanned yet.
    # amount   = NULL means we have no revenue figure at all → useless row → drop.
    before_drop = len(sales_df)
    sales_df["quantity"] = sales_df["quantity"].fillna(0)
    sales_df = sales_df.dropna(subset=["amount"])
    rows_dropped = before_drop - len(sales_df)

    print(f"  [Cleaning] Rows dropped (amount was NULL): {rows_dropped}")
    print(f"  [Cleaning] Cleaned DataFrame shape: {sales_df.shape}")
    logger.info(f"Rows dropped due to null 'amount': {rows_dropped}")

    # ── Task 2.5 — Fix Data Types ─────────────────────────────────────────
    # Pandas sometimes reads dates as plain text strings.
    # We must convert them so we can do date arithmetic (e.g., filter by month).
    sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])
    sales_df["amount"]    = sales_df["amount"].astype(float)
    sales_df["quantity"]  = sales_df["quantity"].astype(int)

    print(f"\n  [Cleaning] Final data types after conversion:")
    print(sales_df.dtypes.to_string())

    return sales_df


# ══════════════════════════════════════════════
# PHASE 3 – DATA TRANSFORMATION
# ══════════════════════════════════════════════

def transform_data(
    sales_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Task 3: Merge DataFrames, calculate revenue, and group by city.

    Step 6 → Merge sales + products + stores.
    Step 7 → Add 'total_revenue' column; calculate mean/max/min with NumPy.
    Step 8 → Group by city → sort by revenue descending.

    Returns:
        Final merged and enriched DataFrame.
    """
    logger.info("═══ TASK 3: Data Transformation ═══")

    # ── Task 3.6 — Merge DataFrames ───────────────────────────────────────
    # merge() is like SQL JOIN.
    # We join sales to products on product_id, then to stores on store_id.
    # how="inner" means only rows that exist in BOTH tables are kept.
    merged_df = (
        sales_df
        .merge(products_df, on="product_id", how="inner")  # adds product_name, category, price
        .merge(stores_df,   on="store_id",   how="inner")  # adds store_name, city, region
    )

    print(f"\n  [Transform] Merged DataFrame shape: {merged_df.shape}")
    print(f"\n  [Transform] Merged DataFrame (first 5 rows):")
    print(merged_df.head().to_string())

    # ── Task 3.7 — Add total_revenue column ───────────────────────────────
    # total_revenue = how much money this single transaction generated.
    # We multiply quantity sold by the official product price (not the billed amount).
    merged_df["total_revenue"] = merged_df["quantity"] * merged_df["price"]

    rev = merged_df["total_revenue"]
    print(f"\n  [Transform] Revenue Statistics (NumPy):")
    print(f"    • Mean Revenue : ₹{np.mean(rev):.2f}")
    print(f"    • Max  Revenue : ₹{np.max(rev):.2f}")
    print(f"    • Min  Revenue : ₹{np.min(rev):.2f}")

    # ── Task 3.8 — Group by City ──────────────────────────────────────────
    # groupby() is like making sub-piles of data.
    # We put all rows for "Mumbai" in one pile, "Delhi" in another, etc.
    # Then we sum up total_revenue for each pile.
    city_revenue = (
        merged_df
        .groupby("city")["total_revenue"]
        .sum()
        .reset_index()
        .rename(columns={"total_revenue": "city_revenue"})
        .sort_values("city_revenue", ascending=False)
    )

    print(f"\n  [Transform] Revenue by City (sorted highest first):")
    print(city_revenue.to_string(index=False))

    # Save intermediate outputs for inspection
    merged_df.to_csv(os.path.join(OUTPUT_DIR, "merged_data.csv"), index=False)
    city_revenue.to_csv(os.path.join(OUTPUT_DIR, "city_revenue.csv"), index=False)

    return merged_df


# ══════════════════════════════════════════════
# PHASE 4 – DATA LOADING INTO SQLITE
# ══════════════════════════════════════════════

def load_to_sqlite(df: pd.DataFrame) -> None:
    """
    Task 4.9: Load the final merged DataFrame into a SQLite database.

    SQLite is a lightweight database stored as a single file on your computer.
    No server needed — perfect for small projects and internship assignments.

    Args:
        df : Merged and cleaned DataFrame to persist.
    """
    logger.info("═══ TASK 4: Loading into SQLite ═══")

    # Connect to (or create) the database file.
    # If retail_sales.db does not exist, SQLite creates it automatically.
    conn = sqlite3.connect(DB_PATH)

    # to_sql() writes the DataFrame as a SQL table.
    # if_exists="replace" → drops the old table and creates a fresh one each run.
    # index=False → we don't want pandas' row numbers as a column.
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    row_count = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {TABLE_NAME}", conn).iloc[0, 0]
    print(f"\n  [SQLite] Table '{TABLE_NAME}' loaded with {row_count} rows.")
    logger.info(f"SQLite: '{TABLE_NAME}' has {row_count} rows.")

    conn.close()


# ══════════════════════════════════════════════
# PHASE 5 – SQL QUERIES
# ══════════════════════════════════════════════

def run_sql_queries() -> dict:
    """
    Task 4.10 & Task 5.11: Run analytical SQL queries on retail_sales table.

    Returns:
        A dictionary with query results as DataFrames.
    """
    logger.info("═══ TASK 4–5: Running SQL Queries ═══")

    conn = sqlite3.connect(DB_PATH)
    results = {}

    # ── Query 1: Top 3 best-selling products by quantity ─────────────────
    # SUM(quantity) adds up all units sold for each product.
    # GROUP BY product_name puts all rows for the same product together.
    # ORDER BY ... DESC puts the biggest number first.
    # LIMIT 3 gives us only the top 3.
    q_top_products = """
        SELECT
            product_name,
            SUM(quantity)     AS total_units_sold,
            SUM(total_revenue) AS total_revenue
        FROM retail_sales
        GROUP BY product_name
        ORDER BY total_units_sold DESC
        LIMIT 3;
    """
    top_products = pd.read_sql(q_top_products, conn)
    results["top_products"] = top_products
    print(f"\n  [SQL] Top 3 Best-Selling Products:")
    print(top_products.to_string(index=False))

    # ── Query 2: Revenue per store per day ────────────────────────────────
    # We group by BOTH store_name AND sale_date.
    # This gives one row per (store, date) combination.
    q_store_day = """
        SELECT
            store_name,
            sale_date,
            SUM(total_revenue) AS daily_revenue
        FROM retail_sales
        GROUP BY store_name, sale_date
        ORDER BY sale_date, daily_revenue DESC;
    """
    store_day = pd.read_sql(q_store_day, conn)
    results["store_daily_revenue"] = store_day
    print(f"\n  [SQL] Revenue per Store per Day:")
    print(store_day.to_string(index=False))

    # ── Query 3: Revenue by city ──────────────────────────────────────────
    q_city = """
        SELECT
            city,
            SUM(total_revenue) AS city_revenue
        FROM retail_sales
        GROUP BY city
        ORDER BY city_revenue DESC;
    """
    city_rev = pd.read_sql(q_city, conn)
    results["city_revenue"] = city_rev
    print(f"\n  [SQL] Revenue by City:")
    print(city_rev.to_string(index=False))

    # ── Query 4: Most profitable product category ─────────────────────────
    q_category = """
        SELECT
            category,
            SUM(total_revenue) AS category_revenue
        FROM retail_sales
        GROUP BY category
        ORDER BY category_revenue DESC
        LIMIT 1;
    """
    top_cat = pd.read_sql(q_category, conn)
    results["top_category"] = top_cat
    print(f"\n  [SQL] Most Profitable Category:")
    print(top_cat.to_string(index=False))

    # ── Query 5: Highest revenue store ────────────────────────────────────
    q_top_store = """
        SELECT
            store_name,
            city,
            SUM(total_revenue) AS store_revenue
        FROM retail_sales
        GROUP BY store_name
        ORDER BY store_revenue DESC
        LIMIT 1;
    """
    top_store = pd.read_sql(q_top_store, conn)
    results["top_store"] = top_store
    print(f"\n  [SQL] Highest Revenue Store:")
    print(top_store.to_string(index=False))

    # ── Query 6: Average order value ─────────────────────────────────────
    q_aov = """
        SELECT
            ROUND(AVG(total_revenue), 2) AS avg_order_value
        FROM retail_sales;
    """
    aov = pd.read_sql(q_aov, conn)
    results["avg_order_value"] = aov
    print(f"\n  [SQL] Average Order Value:")
    print(aov.to_string(index=False))

    # ── Query 7: Products never sold ─────────────────────────────────────
    # LEFT JOIN keeps ALL products even if they have no matching sale.
    # WHERE rs.product_id IS NULL means no match was found → never sold.
    q_unsold = """
        SELECT p.product_id, p.product_name
        FROM (SELECT DISTINCT product_id, product_name FROM retail_sales) p
        -- In a real scenario you'd JOIN the products master table here.
        -- Demonstrating concept: find products with zero quantity sold.
        WHERE p.product_id NOT IN (
            SELECT DISTINCT product_id
            FROM retail_sales
            WHERE quantity > 0
        );
    """
    unsold = pd.read_sql(q_unsold, conn)
    results["unsold_products"] = unsold
    print(f"\n  [SQL] Products Never Sold (quantity = 0):")
    print(unsold.to_string(index=False) if not unsold.empty else "    → All products have been sold.")

    conn.close()
    return results


# ══════════════════════════════════════════════
# PHASE 6 – SUMMARY REPORT
# ══════════════════════════════════════════════

def generate_report(df: pd.DataFrame, sql_results: dict,
                    duplicates_removed: int, rows_dropped: int) -> None:
    """
    Task 5.12: Print a professional terminal summary report.

    Args:
        df                 : Final merged DataFrame.
        sql_results        : Dictionary of query results from run_sql_queries().
        duplicates_removed : Count of duplicate rows removed during cleaning.
        rows_dropped       : Count of rows dropped due to null 'amount'.
    """
    logger.info("═══ TASK 5: Generating Report ═══")

    total_transactions = len(df)
    total_revenue      = df["total_revenue"].sum()
    avg_revenue        = df["total_revenue"].mean()

    top_product = (
        df.groupby("product_name")["total_revenue"].sum().idxmax()
    )
    top_store = (
        df.groupby("store_name")["total_revenue"].sum().idxmax()
    )
    top_city = (
        df.groupby("city")["total_revenue"].sum().idxmax()
    )

    report_lines = [
        "",
        "═" * 45,
        "       RetailMart Daily Sales Report",
        "═" * 45,
        f"  Generated On       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "─" * 45,
        f"  Total Transactions : {total_transactions}",
        f"  Total Revenue      : ₹{total_revenue:,.2f}",
        f"  Average Revenue    : ₹{avg_revenue:,.2f}",
        "─" * 45,
        f"  Top Selling Product: {top_product}",
        f"  Top Selling Store  : {top_store}",
        f"  Top Selling City   : {top_city}",
        "─" * 45,
        f"  Duplicates Removed : {duplicates_removed}",
        f"  Null Rows Dropped  : {rows_dropped}",
        "═" * 45,
        ""
    ]

    report_text = "\n".join(report_lines)
    print(report_text)

    # Save report to file
    report_path = os.path.join(OUTPUT_DIR, "report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info(f"Report saved → {report_path}")


# ══════════════════════════════════════════════
# PHASE 7 – FULL PIPELINE  (Task 6.13 & 6.14)
# ══════════════════════════════════════════════

def run_pipeline() -> None:
    """
    Task 6.13: Master function that runs the complete data pipeline end-to-end.

    Steps:
        1. Ingest CSV files.
        2. Clean sales data.
        3. Transform and merge.
        4. Load into SQLite.
        5. Run SQL queries.
        6. Generate report.

    Task 6.14: Each step is wrapped in try-except so a single failure
    does not crash the entire pipeline silently.
    """
    print("\n" + "█" * 50)
    print("  RetailMart Data Pipeline — STARTING")
    print("█" * 50)
    logger.info("Pipeline started.")

    # ── Step 1: Ingest ────────────────────────────────────────────────────
    try:
        sales_df, products_df, stores_df = ingest_data()
    except FileNotFoundError as e:
        logger.error(f"INGESTION FAILED — {e}")
        print(f"\n  ❌ ERROR: {e}")
        print("  → Please make sure all three CSV files exist in the 'data/' folder.")
        return   # stop pipeline — no point continuing without data
    except ValueError as e:
        logger.error(f"EMPTY FILE — {e}")
        print(f"\n  ❌ ERROR: {e}")
        return

    # Track cleaning stats for the final report
    duplicates_removed = sales_df.duplicated().sum()

    # ── Step 2: Clean ─────────────────────────────────────────────────────
    try:
        rows_before_drop = len(sales_df.drop_duplicates())
        cleaned_sales    = clean_data(sales_df)
        rows_dropped     = rows_before_drop - len(cleaned_sales)
    except KeyError as e:
        logger.error(f"CLEANING FAILED — Column not found: {e}")
        print(f"\n  ❌ ERROR: Expected column missing → {e}")
        return
    except Exception as e:
        logger.error(f"CLEANING FAILED — {e}")
        print(f"\n  ❌ Unexpected error during cleaning: {e}")
        return

    # ── Step 3: Transform & Merge ─────────────────────────────────────────
    try:
        merged_df = transform_data(cleaned_sales, products_df, stores_df)
    except Exception as e:
        logger.error(f"TRANSFORMATION FAILED — {e}")
        print(f"\n  ❌ Error during transformation: {e}")
        return

    # Export cleaned merged CSV
    try:
        export_path = os.path.join(OUTPUT_DIR, "cleaned_sales.csv")
        merged_df.to_csv(export_path, index=False)
        logger.info(f"Cleaned CSV exported → {export_path}")
    except IOError as e:
        logger.warning(f"Could not export CSV: {e}")

    # ── Step 4: Load to SQLite ────────────────────────────────────────────
    try:
        load_to_sqlite(merged_df)
    except sqlite3.Error as e:
        logger.error(f"SQLITE ERROR — {e}")
        print(f"\n  ❌ Database error: {e}")
        return

    # ── Step 5: SQL Analytics ─────────────────────────────────────────────
    try:
        sql_results = run_sql_queries()
    except sqlite3.Error as e:
        logger.error(f"SQL QUERY ERROR — {e}")
        print(f"\n  ❌ SQL error: {e}")
        return

    # ── Step 6: Report ────────────────────────────────────────────────────
    try:
        generate_report(merged_df, sql_results, duplicates_removed, rows_dropped)
    except Exception as e:
        logger.error(f"REPORT ERROR — {e}")
        print(f"\n  ❌ Could not generate report: {e}")

    print("█" * 50)
    print("  Pipeline COMPLETE. Outputs saved to 'output/' folder.")
    print("█" * 50 + "\n")
    logger.info("Pipeline finished successfully.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()
