# Traffic Collision Data Analysis using Azure Synapse Analytics Dedicated SQL Pool

## Project Overview

This project demonstrates how to process, analyze, and derive insights from traffic collision data using Azure Synapse Analytics and ADLS Gen2. The pipeline involves:
1. Extracting JSON data from a REST API.
2. Transforming and storing the data in CSV format in Azure Data Lake Storage Gen2.
3. Bulk loading the data into a Dedicated SQL Pool for analysis.

---

## **Tech Stack**

- **Azure Synapse Analytics**
  - Dedicated SQL Pool
  - Pipelines
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**
- **Data Format**: JSON → CSV
- **Programming**: SQL

---

## **Dataset**

The data contains information about motor vehicle collisions in New York City.  
**Source**: [NYC Open Data - Motor Vehicle Collisions](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)  
- **Rows**: 2.15 million  
- **Columns**: 29  
- **API Endpoint**: `https://data.cityofnewyork.us/resource/h9gi-nx95.json`

---

## **Pipeline Steps**

### **1. Extract Data from REST API**
- **Source**: NYC Open Data API.
- Created a **Linked Service** in Synapse Pipelines to connect to the REST API endpoint.
- Used a **Copy Data Activity** in the pipeline to pull JSON data into ADLS Gen2.

### **2. Transform Data**
- Converted JSON data into CSV format using Synapse Pipeline.
- Stored the transformed CSV data in an ADLS Gen2 container.

### **3. Create Dedicated SQL Pool**
- Configured a Dedicated SQL Pool in Synapse Analytics for high-performance querying.

### **4. Bulk Load Data into Dedicated SQL Pool**
- Created a table schema in the SQL Pool matching the dataset structure.
- Used the `COPY INTO` statement to bulk load data from ADLS Gen2 into the SQL Pool.

#### **Table Schema**
```sql
CREATE TABLE dbo.CrashData (
    crash_date DATETIME,
    on_street_name NVARCHAR(255),
    number_of_persons_injured INT,
    number_of_persons_killed INT,
    number_of_pedestrians_injured INT,
    number_of_pedestrians_killed INT,
    number_of_cyclist_injured INT,
    number_of_cyclist_killed INT,
    number_of_motorist_injured INT,
    number_of_motorist_killed INT,
    contributing_factor_vehicle_1 NVARCHAR(255),
    contributing_factor_vehicle_2 NVARCHAR(255),
    collision_id INT,
    vehicle_type_code1 NVARCHAR(255)
);
```
---

## **Data Analysis**

### **Key Insights:**

1. **Top 5 Streets with Most Collisions**
   - Identified streets with the highest number of collisions.

2. **Total Injuries and Deaths per Vehicle Type**
   - Grouped by vehicle types like Sedan, Moped, etc.

3. **Monthly Collision Trend**
   - Trends over months to spot seasonal patterns.

4. **Contributing Factors for Injuries**
   - Insights into factors like Aggressive Driving or Slippery Pavement.

5. **Collisions Involving Pedestrians**
   - Analyzed incidents involving pedestrians to identify high-risk zones.

6. **Yearly Collisions by Severity**
   - Aggregated yearly data to summarize injury and death counts.
