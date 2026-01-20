# 🛠 Python ETL Pipeline

This repository demonstrates a simple **ETL (Extract, Transform, Load)** pipeline in Python. It extracts data from an API, transforms it using `pandas`, 
and loads it into a CSV file for analysis and Integration.

## 🚀 **Features**
  - Simple and clean ETL structure
  - Easy to customize for different APIs
- **Extract**: Fetch JSON data from an API using `requests`
- **Transform**: Clean and structure data using `pandas`
- **Load**: Save transformed data into a CSV file
- Modular design for easy customization and scalability

## 🚀 Getting Started
## ✅ **Prerequisites**
- Python **3.7+**
- Install required libraries
- pip install requests pandas

## ▶️ Usage
Run the ETL pipeline:
  python main.py
Modify:
  API endpoint in api_client.py
  Transformation logic in transform.py
  Output file path in load() function

## 🧩 How It Works
Extract: Fetch data from API using extract() in api_client.py
Transform: Convert JSON to DataFrame and clean data using transform() in transform.py
Load: Save DataFrame to CSV using load() in transform.py

## 📂 **Project Structure**
```
Python_ETL_Pipeline/
│
├── api_client.py           # Extract logic
├── transform.py            # Transform & Load logic
├── main.py                 # ETL orchestration
└── data/                   # Output CSV files
```
## 🙌 Contributing
Feel free to fork the repo and submit pull requests for improvements or new features.

## 🙌 Author
**Prashant**  
Python | Data Engineering | ETL
