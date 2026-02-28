# **Dislog Group Data Analysis**

Welcome to the **Data Analysis By Dislog Group**! This repository is designed to help trainees learn data warehousing, SQL querying, and data analysis using a structured database schema. The goal is to transform raw transactional data into actionable insights.

---

## **Table of Contents**
1. [Introduction](#introduction)
2. [Dataset](#dataset)
3. [Database Schema](#database-schema)
4. [Data Confidentiality](#data-confidentiality)
5. [Analysis Subjects](#analysis-subjects)
6. [Getting Started](#getting-started)
7. [Collaboration Guidelines](#collaboration-guidelines)

---

## **Introduction**

This project provides a simplified yet realistic database schema for a sales and invoicing system. The schema is designed to simulate a real-world scenario where trainees can practice building a data warehouse and performing various types of data analysis.

The dataset includes information about customers, products, sales, promotions, and invoices. Trainees will use this data to:
- Build a data warehouse.
- Perform exploratory data analysis (EDA).
- Generate insights and visualizations.

---

## **Dataset**

The dataset used in this project is hosted on Google Drive. Follow these steps to download and use it:

1. **Access the Dataset**:
   - Visit the following link: [Google Drive Folder](https://drive.google.com/drive/folders/1gHgJSyr3DrrPCzMA5PZHGKpZejUjm47D).
   - If you do not have access to the folder, please request permission by contacting the repository administrator or sending an email to [o.guemmar@dislogroup.com](o.guemmar@dislogroup.com) or [o.daoud@dislogroup.com](o.daoud@dislogroup.com).
   - Download the required files and save them to the `data/` directory in your local repository.

2. **File Encoding Note**:
   - Some files, such as `SalesLine.csv` and `Invoices.csv`, are encoded in **ANSI (1252)** instead of **UTF-8**.
   - If you encounter encoding issues while loading these files, you may need to convert them to UTF-8 using a text editor or a script. For example, in Python:
     ```python
     # Convert ANSI (1252) to UTF-8
     with open("data/SalesLine.csv", "r", encoding="cp1252") as infile:
         content = infile.read()
     with open("data/SalesLine_utf8.csv", "w", encoding="utf-8") as outfile:
         outfile.write(content)
     ```

3. **Verify the Files**:
   - Ensure all files are correctly placed in the `data/` directory before proceeding with the analysis.

---

## **Database Schema**

The database consists of the following tables:

### **Dimension Tables**
1. **Region**
   - `regionid`: Unique identifier for the region.
   - `description`: Description of the region.

2. **Sector**
   - `sectorid`: Unique identifier for the sector.
   - `description`: Description of the sector.

3. **Customer**
   - `accountid`: Unique identifier for the customer.
   - `accountname`: Name of the customer.
   - `regionid`: Foreign key referencing `Region`.
   - `sectorid`: Foreign key referencing `Sector`.

4. **Seller**
   - `sellerid`: Unique identifier for the seller.
   - `sellername`: Name of the seller.

5. **Product**
   - `itemid`: Unique identifier for the product.
   - `name`: Name of the product.
   - `namealias`: Alias name of the product.
   - `marque`: Brand of the product.

### **Fact Tables**
1. **SalesHeader**
   - `saleid`: Unique identifier for the sale.
   - `accountid`: Foreign key referencing `Customer`.
   - `sellerid`: Foreign key referencing `Seller`.
   - `orderdate`: Date of the order.
   - `delivdate`: Delivery date.
   - `bruteamount`: Total brute amount.
   - `netamount`: Net amount.
   - `taxamount`: Tax amount.
   - `totalamount`: Total amount.

2. **SalesLine**
   - `saleid`: Foreign key referencing `SalesHeader`.
   - `itemid`: Foreign key referencing `Product`.
   - `qty`: Quantity sold.
   - `unitprice`: Price per unit.
   - `httotalamount`: Net total amount.
   - `ttctotalamount`: Total amount including taxes.
   - `promotype`: Type of promotion.
   - `promovalue`: Value of the promotion.

3. **Invoice**
   - `invoiceid`: Unique identifier for the invoice.
   - `salesid`: Foreign key referencing `SalesHeader`.
   - `paymentamount`: Payment amount.
   - `paymentmethod`: Payment method code (`001`, `102`, `205`).

---

## **Data Confidentiality**

**IMPORTANT**: The data provided in this repository is confidential and must not be shared publicly or used outside the scope of this training project. Any unauthorized sharing or misuse of the data will result in disciplinary action.

- **Allowed Actions**:
  - Trainees may share their work (e.g., queries, scripts, visualizations) publicly **only if** they anonymize the data and remove any sensitive information.
  - Collaboration within the GitHub repository is encouraged.

- **Prohibited Actions**:
  - Sharing raw data files or credentials.
  - Publishing sensitive information about customers, sellers, or transactions.

---

## **Analysis Subjects**

Below are several analysis subjects that trainees can work on. Each subject focuses on a specific aspect of the data and provides opportunities to practice SQL querying, data transformation, and visualization.

---

### **Subject 1: Advanced Sales Performance Analysis**
- **Objective**: Perform a multi-dimensional analysis of sales performance, incorporating time-series, geographic, and product-level insights.
- **Tasks**:
  1. **Sales Trends Over Time**:
     - Analyze monthly, quarterly, and yearly sales trends.
     - Identify seasonality patterns (e.g., peak sales months).
  2. **Geographic Analysis**:
     - Calculate total sales grouped by region and sector.
     - Compare sales growth rates across regions and sectors over time.
  3. **Top Performing Sellers**:
     - Rank sellers based on total sales, average order value, and number of orders.
     - Identify outliers (e.g., sellers with unusually high or low performance).
  4. **Customer Retention**:
     - Measure customer retention rates by analyzing repeat purchases over time.
     - Segment customers into "New" and "Returning" based on their purchase history.
  5. **Visualization**:
     - Create dashboards showing sales trends, geographic distribution, and seller performance.
     - Use tools like Power BI, Tableau, or Python libraries (Matplotlib/Seaborn) for visualization.
- **Skills Practiced**:
  - Time-series analysis (`DATEPART`, `DATEDIFF`).
  - Aggregation and ranking (`SUM`, `GROUP BY`, `RANK`).
  - Joins (`SalesHeader`, `Customer`, `Seller`, `Region`, `Sector`).
  - Visualization and dashboard creation.

---

### **Subject 2: Customer Lifetime Value (CLV) and Segmentation**
- **Objective**: Develop a comprehensive understanding of customer value and behavior using advanced segmentation techniques.
- **Tasks**:
  1. **Calculate CLV**:
     - Define CLV as the total revenue generated by a customer over their lifetime.
     - Use historical data to estimate future CLV (optional: apply predictive modeling).
  2. **Segmentation**:
     - Segment customers into tiers such as "High Value," "Medium Value," and "Low Value" based on CLV.
     - Add behavioral dimensions such as purchase frequency, average order value, and recency of purchase (RFM analysis).
  3. **Loyalty Analysis**:
     - Identify loyal customers who consistently make purchases.
     - Analyze the impact of loyalty programs (if applicable) on customer retention.
  4. **Churn Prediction**:
     - Predict which customers are at risk of churning based on their recent activity.
     - Suggest strategies to re-engage these customers.
  5. **Visualization**:
     - Create a heatmap showing customer segments by region and sector.
     - Build a Pareto chart to highlight the contribution of top customers to total revenue.
- **Skills Practiced**:
  - Statistical analysis (mean, median, standard deviation).
  - RFM segmentation and clustering techniques.
  - Predictive modeling (optional: regression, classification).
  - Data visualization.

---

### **Subject 3: Product Profitability and Cross-Selling Analysis**
- **Objective**: Evaluate product profitability and identify opportunities for cross-selling and upselling.
- **Tasks**:
  1. **Profitability Analysis**:
     - Calculate gross profit for each product (revenue minus cost).
     - Identify products with the highest and lowest profit margins.
  2. **Cross-Selling Opportunities**:
     - Analyze co-purchase patterns (e.g., products frequently bought together).
     - Recommend cross-selling strategies based on these patterns.
  3. **Product Bundling**:
     - Identify complementary products that can be bundled for promotions.
     - Evaluate the impact of bundling on sales and profitability.
  4. **Promotion Effectiveness**:
     - Analyze how promotions affect product profitability.
     - Identify products where promotions significantly boost sales but reduce profitability.
  5. **Visualization**:
     - Create a scatter plot showing profitability vs. sales volume for each product.
     - Build a network graph to visualize co-purchase relationships.
- **Skills Practiced**:
  - Joins (`SalesLine`, `Product`).
  - Conditional filtering (`CASE` statements).
  - Association rule mining (optional: Apriori algorithm).
  - Visualization of product relationships.

---

### **Subject 4: Payment Behavior and Risk Analysis**
- **Objective**: Analyze payment behavior and assess financial risks associated with delayed or missed payments.
- **Tasks**:
  1. **Payment Trends**:
     - Analyze payment amounts and methods over time.
     - Identify trends in payment method preferences (e.g., cash vs. credit card).
  2. **Late Payments**:
     - Identify invoices with delayed payments and calculate the average delay.
     - Segment customers based on their payment punctuality.
  3. **Risk Assessment**:
     - Develop a risk score for customers based on payment history and outstanding balances.
     - Predict the likelihood of payment defaults using historical data (optional: machine learning).
  4. **Optimization**:
     - Suggest improvements to payment terms or incentives to reduce delays.
  5. **Visualization**:
     - Create a bar chart showing payment method distribution.
     - Build a heat map highlighting regions or sectors with high payment delays.
- **Skills Practiced**:
  - Date calculations (`DATEDIFF`).
  - Aggregation and grouping.
  - Predictive modeling (optional: logistic regression).
  - Visualization of payment trends and risks.

---

### **Subject 5: Promotional Campaign Optimization**
- **Objective**: Optimize promotional campaigns to maximize ROI and customer engagement.
- **Tasks**:
  1. **Campaign Performance**:
     - Compare sales volumes and revenues for products with and without promotions.
     - Evaluate the effectiveness of different promotion types (`promotype`) and values (`promovalue`).
  2. **Customer Response**:
     - Analyze how different customer segments respond to promotions.
     - Identify segments that are most influenced by discounts.
  3. **ROI Calculation**:
     - Calculate the return on investment (ROI) for each promotion type.
     - Identify promotions with negative ROI and suggest alternatives.
  4. **Recommendations**:
     - Propose changes to the promotional strategy based on findings.
     - Design targeted campaigns for specific customer segments.
  5. **Visualization**:
     - Create a stacked bar chart comparing sales with and without promotions.
     - Build a scatter plot showing ROI vs. promotion type.
- **Skills Practiced**:
  - Filtering and grouping by `promotype`.
  - Calculating KPIs such as ROI and conversion rates.
  - Writing actionable recommendations.

---

### **Subject 6: Market Basket Analysis and Recommendation Systems**
- **Objective**: Perform market basket analysis to uncover hidden purchasing patterns and build a recommendation system.
- **Tasks**:
  1. **Association Rules**:
     - Use association rule mining (e.g., Apriori algorithm) to identify frequently co-purchased items.
     - Calculate metrics such as support, confidence, and lift.
  2. **Recommendation System**:
     - Build a collaborative filtering-based recommendation system to suggest products to customers.
     - Alternatively, use content-based filtering based on product attributes.
  3. **Impact Analysis**:
     - Evaluate the potential impact of implementing these recommendations on sales.
  4. **Visualization**:
     - Create a network graph showing product relationships.
     - Build a dashboard to display personalized recommendations for customers.
- **Skills Practiced**:
  - Association rule mining.
  - Recommendation system algorithms (collaborative filtering, content-based filtering).
  - Visualization of product relationships.

---

## **Getting Started**

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/og-disloggroup/dislog-group-it-trainees-program.git
   cd data-analysis-training
   ```

2. **Download the Dataset**:
   - Visit the [Google Drive Folder](https://drive.google.com/drive/folders/1gHgJSyr3DrrPCzMA5PZHGKpZejUjm47D).
   - Download the dataset files and save them to the `data/` directory.

3. **Set Up the Database**:
   - Use the provided SQL scripts to create the database schema and populate it with sample data.
   - Ensure you have access to a SQL Server instance or another compatible database system.

4. **Choose a Subject**:
   - Select one of the analysis subjects from the list above.
   - Write SQL queries, perform transformations, and generate insights.

5. **Visualize Results**:
   - Use tools like Power BI, Tableau, or Python libraries (Matplotlib, Seaborn) to visualize your findings.

---

## **Collaboration Guidelines**

1. **Adding New Trainees**:
   - Add new trainees as collaborators to the repository.
   - Provide them with access to the database and necessary credentials.

2. **Branching Strategy**:
   - Create a new branch for each trainee or subject.
   - Example:
     ```bash
     git checkout -b oussama-dsg-i2tp.
     ```

3. **Commit Messages**:
   - Use clear and descriptive commit messages.
   - Example: `Add SQL queries for sales performance analysis`.

4. **Pull Requests**:
   - Submit pull requests for review before merging changes into the main branch.

5. **Code Reviews**:
   - Encourage peer reviews to ensure code quality and adherence to best practices.

---

## **Conclusion**

This project is a valuable learning opportunity for trainees to gain hands-on experience with data warehousing and analysis. By working on these subjects, trainees will develop essential skills in SQL, data transformation, and visualization. Remember to respect data confidentiality and follow the guidelines outlined in this README.

Happy analyzing! 🚀