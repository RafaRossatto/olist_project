# ETL of Metrics from the Brazilian E-Commerce Public Dataset

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## 📌 Problem the Project Solves

E-commerce platforms generate large volumes of operational data, but raw data alone doesn't provide actionable insights. This project solves the problem of transforming raw order data from the Brazilian E-Commerce Public Dataset into meaningful business metrics. Specifically, it calculates key performance indicators (KPIs) such as approval time, delivery accuracy, transit time, and total delivery time — helping identify bottlenecks, improve delivery estimates, and enhance customer experience.

> **Dataset source:** [Brazilian E-Commerce Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv)

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python 3** | Core programming language |
| **Pandas** | Data manipulation and analysis |
| **Argparse** | Command-line argument parsing |
| **CSV** | Input/output data format |
| **OOP** | Modular and reusable code structure |

## 🚀 How to Run or Access the Project

### Prerequisites

```bash
# Clone the repository
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

```

## Execution

Run the command:

```bash
python3 main.py <input_file> <output_path> <metric>
```
### Metric Options

- `time_to_approved` - Time taken for a purchase to be approved
- `transit_time` - Time the product spends in transit
- `comparation_time` - Difference between estimated and actual delivery date
- `total_time` - Total time from order creation to customer delivery
- `all` - Generates all of the above metrics

## ✨ Main Functionalities

- Reads raw order data from a CSV file
- Calculates four delivery-related metrics using OOP
- Saves each metric as a separate CSV file in the specified output directory
- Supports selective metric calculation or batch processing (`all`)
- Uses `argparse` for flexible command-line usage

## 📈 Next Steps / Improvements

- Add unit tests for each metric calculation
- Integrate with a visualization dashboard (Streamlit or Power BI)
- Support for database output (PostgreSQL, MySQL) instead of only CSV
- Optimize performance for very large datasets (chunk processing)
- Package the project as a pip-installable module
- Add logging and error handling for missing or malformed data
- Dockerize the application for one-click reproducibility
