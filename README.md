

# 🌐 dfexplorer

**dfexplorer** is a versatile, cross-platform toolkit for powerful exploratory data analysis (EDA) on structured datasets. With implementations in **Python** 🐍, **R** 📊, and **PySpark** 🚀, dfexplorer brings detailed data insights across various environments. We’re excited to expand to other platforms in future releases!

## 🔍 Overview

**dfexplorer** provides comprehensive EDA capabilities to help you understand the characteristics of your dataset. It generates insights into character, numeric, and date fields, making it easy to identify patterns, missing data, and distributions.

### 🛠️ Features

- **Character Fields**:
  
  - 📝 Total record and missing value counts
  - 📉 Percentage of missing values
  - 🔢 Frequency tables of unique values

- **Numeric Fields**:
  
  - 🧮 Total record and missing value counts
  - 📊 Descriptive statistics: min, max, mean, and mode
  - 📈 Percentile distribution (1st, 5th, 10th, 20th, 25th, 50th, 75th, 80th, 90th, 95th, 99th)

- **Date Fields**:
  
  - 📆 Total record and missing value counts
  - 📅 Minimum and maximum dates
  - 📊 Aggregated counts by Year-Month

Each version of dfexplorer includes an optional export feature, allowing you to save results in Excel for easy sharing and further analysis 📂.

---

## 🚀 Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/dfexplorer.git
cd dfexplorer
```

### Step 2: Install Dependencies

- For **Python** 🐍 and **PySpark** 🚀, install dependencies from `requirements.txt`:
  
  ```bash
  pip install -r requirements.txt
  ```

- For **R** 📊, install the required packages:
  
  ```R
  install.packages(c("dplyr", "tidyr", "openxlsx"))
  ```

---

## 📖 Usage

**dfexplorer** offers consistent functionality across each platform. Example usage for each environment is provided in the `examples/` directory for Python, R, and PySpark.

### Python 🐍

```python
from python.dfexplorer import dfexplorer
import pandas as pd

df = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie', 'David', None],
    'Age': [24, 27, 35, None, 29],
    'JoinDate': pd.to_datetime(['2020-05-01', '2019-03-20', '2021-01-15', '2022-02-11', None])
})

# Run analysis and export to Excel if needed
dfexplorer(df, output_file='output_analysis.xlsx', export=True)
```

### R 📊

```R
source('r/dfexplorer.R')

df <- data.frame(
    Name = c('Alice', 'Bob', 'Charlie', 'David', NA),
    Age = c(24, 27, 35, NA, 29),
    JoinDate = as.Date(c('2020-05-01', '2019-03-20', '2021-01-15', '2022-02-11', NA))
)

# Run analysis with optional export
dfexplorer(df, "output_analysis.xlsx", export = TRUE)
```

### PySpark 🚀

```python
from pyspark.sql import SparkSession
from pyspark.dfexplorer_pyspark import dfexplorer

spark = SparkSession.builder.appName("dfexplorer").getOrCreate()

data = [("Alice", 24, "2020-05-01"), ("Bob", 27, "2019-03-20"), ("Charlie", 35, "2021-01-15"), ("David", None, "2022-02-11")]
columns = ["Name", "Age", "JoinDate"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("JoinDate", df["JoinDate"].cast("date"))

# Run analysis with optional export
dfexplorer(df, output_file="output_analysis.xlsx", export=True)
```

---

## 🔮 Future Development

We’re planning to expand **dfexplorer** to additional platforms 🌍, making it accessible across even more data environments. Stay tuned for updates and new features!

## 🙌 Contributing

We welcome contributions! If you find a bug 🐞, have suggestions 💡, or want to discuss enhancements, please [open an issue](https://github.com/iiaiac/dfexplorer/issues), [a pull request](https://github.com/iiaiac/dfexplorer/pulls) or start a [discussion](https://github.com/iiaiac/dfexplorer/discussions). Contributions to the codebase are also appreciated – we’re excited to review your pull requests 🤝.

---

## 📄 License

This project is licensed under the MIT License.

---
