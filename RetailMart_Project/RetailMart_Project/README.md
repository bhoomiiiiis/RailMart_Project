# 🛒 RetailMart Sales Data Pipeline

A complete end-to-end Data Engineering pipeline built with Python, Pandas, NumPy, and SQLite.

> Built as part of the RetailMart Pvt. Ltd. Junior Data Engineer internship assignment.

---

## 📌 Project Description

RetailMart collects daily sales transactions from 5 stores across India. This pipeline ingests raw, messy CSV data, cleans it, merges it with product and store master data, loads it into a SQLite database, runs analytical SQL queries, and generates a business summary report.

---

## ✨ Features

- ✅ Loads and validates three CSV source files
- ✅ Detects and removes duplicate transactions
- ✅ Handles missing values with business logic (quantity → 0, drop null amounts)
- ✅ Converts and enforces correct data types
- ✅ Merges sales, products, and stores into one analysis-ready table
- ✅ Calculates `total_revenue` using NumPy
- ✅ Groups revenue by city, store, and product using SQL and pandas
- ✅ Loads final data into SQLite (`retail_sales.db`)
- ✅ Runs 7 optimized SQL analytics queries
- ✅ Generates a formatted terminal + file report
- ✅ Full error handling with meaningful messages
- ✅ Structured logging to `pipeline.log`

---

## 🛠️ Technologies Used

| Tool       | Purpose                              |
|------------|--------------------------------------|
| Python 3.x | Core programming language            |
| Pandas     | Data loading, cleaning, merging      |
| NumPy      | Statistical calculations             |
| SQLite3    | Lightweight relational database      |
| Logging    | Runtime audit trail                  |

---

## 📁 Folder Structure

```
RetailMart_Project/
│
├── data/
│   ├── sales_data.csv      ← Raw daily transactions (with intentional errors)
│   ├── products.csv        ← Product master data
│   └── stores.csv          ← Store master data
│
├── output/
│   ├── cleaned_sales.csv   ← Final merged & cleaned dataset
│   ├── merged_data.csv     ← Merged before revenue column is added
│   ├── city_revenue.csv    ← Revenue aggregated by city
│   └── report.txt          ← Terminal report saved to file
│
├── retail_sales.db         ← SQLite database (auto-created on run)
├── pipeline.py             ← Main pipeline script
├── requirements.txt        ← Python dependencies
├── pipeline.log            ← Auto-generated runtime log
└── README.md               ← You are here
```

---

## ⚙️ Installation

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/retailmart-pipeline.git
cd retailmart-pipeline

# 2. (Optional) Create a virtual environment
python -m venv venv
source venv/bin/activate       # macOS / Linux
venv\Scripts\activate          # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## 🚀 How to Run

```bash
python pipeline.py
```

The pipeline runs all steps automatically and prints progress to the terminal.  
Check `output/` for exported files and `pipeline.log` for the full audit trail.

---

## 📊 Sample Output

```
═════════════════════════════════════════════
       RetailMart Daily Sales Report
═════════════════════════════════════════════
  Generated On       : 2024-01-20 10:30:00
─────────────────────────────────────────────
  Total Transactions : 16
  Total Revenue      : ₹13,979.00
  Average Revenue    : ₹873.69
─────────────────────────────────────────────
  Top Selling Product: Samsung Galaxy M14
  Top Selling Store  : RetailMart Banjara Hills
  Top Selling City   : Hyderabad
─────────────────────────────────────────────
  Duplicates Removed : 3
  Null Rows Dropped  : 2
═════════════════════════════════════════════
```

---

## 🗄️ SQL Queries

### Top 3 Products by Quantity Sold
```sql
SELECT product_name, SUM(quantity) AS total_units_sold
FROM retail_sales
GROUP BY product_name
ORDER BY total_units_sold DESC
LIMIT 3;
```

### Revenue per Store per Day
```sql
SELECT store_name, sale_date, SUM(total_revenue) AS daily_revenue
FROM retail_sales
GROUP BY store_name, sale_date
ORDER BY sale_date, daily_revenue DESC;
```

### Revenue by City
```sql
SELECT city, SUM(total_revenue) AS city_revenue
FROM retail_sales
GROUP BY city
ORDER BY city_revenue DESC;
```

---

## 🔮 Future Improvements

- [ ] Replace SQLite with PostgreSQL for production scale
- [ ] Add Airflow DAG to schedule daily pipeline runs
- [ ] Add data validation layer using Great Expectations
- [ ] Visualise revenue trends using Matplotlib / Plotly
- [ ] Containerise with Docker for consistent deployment
- [ ] Write unit tests with `pytest`

---

## 👤 Author

Built with ❤️ for the RetailMart Data Engineering Internship Assignment.
