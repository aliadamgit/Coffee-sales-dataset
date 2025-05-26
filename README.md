# Coffee-sales-dataset
![Copy of Microsoft Fabric and Power BI](https://github.com/user-attachments/assets/bca1d657-e3eb-4d4d-9d05-93d9b629b614)

Data was fetched from the GitHub repository using Microsoft Fabric Data Factory to transfer data to the fabric.

Objectives of Analysis
Transaction data analysis aims to provide important insights into customer behavior, product popularity, sales patterns, and operational efficiency. It also aims to improve inventory management, enhance decision-making processes, and identify sales opportunities.

## Requirements:
- Collect, clean, and prepare data
- Analyze monthly and daily sales patterns
- Identify high-performance days and times
- Develop engaging offer reports
- Create interactive dashboards

## Data Source
[Maven Analytics](https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&search=coffee%20shop) Data Warehouse
Number of records used: 149,116

<img width="105" alt="pipline" src="https://github.com/user-attachments/assets/fec1afa8-d51e-46fc-8fa3-f80dc3f49940" />
ت
Data Transformation
مع
Silver Lakehouse 

```python
# importing the required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

```python
# Reading the data from Brinze Lakehouse
df_categories = spark.read.table("BronzeLH.Categories")
display(df_categories)
```
![image](https://github.com/user-attachments/assets/8d516a17-9273-473f-80cc-5526ed3bafb1)

```python
# Convert the data type of the column
df_categories = df_categories.withColumn("CategoryId",col("CategoryId").cast(IntegerType()))
```


```python
# Write the data into Silver Lakehouse as selt table
df_categories.write.format("delta")\
       .mode("overwrite")\
       .saveAsTable("SilverLH.Dim_Categories")
```

# Importing Coffee Shop Sales Table

```python
# Reading the Sales Table from Brinze Lakehouse
df_CoffeeSales = spark.read.table("BronzeLH.CoffeeSales")
display(df_CoffeeSales.head(1))
```
![image](https://github.com/user-attachments/assets/bc56248f-be83-44f8-9cca-31a09d76b52c)

```python
# Convert the data type of the columns
df_CoffeeSales = df_CoffeeSales.withColumn("transaction_id",col("transaction_id").cast(IntegerType()))\
          .withColumn("transaction_qty",col("transaction_qty").cast(IntegerType()))\
          .withColumn("store_id",col("store_id").cast(IntegerType()))\
          .withColumn("product_id",col("product_id").cast(IntegerType()))\
          .withColumn("unit_price",col("unit_price").cast(IntegerType()))\
          .withColumn("CategoryId",col("CategoryId").cast(IntegerType()))\
          .withColumn("Product_TypeId",col("Product_TypeId").cast(IntegerType()))
```

```python
# Create Calculated Column as Total Sales
df_CoffeeSales = df_CoffeeSales.withColumn('TotalSales', df_CoffeeSales.transaction_qty * df_CoffeeSales.unit_price)
display(df_CoffeeSales.head(3))
```
![image](https://github.com/user-attachments/assets/c8fb5001-6953-4910-bbcc-4a3c9bceeaf4)

```python
# Add Column using when otherwise condition
from pyspark.sql.functions import lit

from pyspark.sql.functions import when
df_CoffeeSales = df_CoffeeSales.withColumn("grade_class", \
   when((df_CoffeeSales.TotalSales < 5 ), lit("Low")) \
     .when((df_CoffeeSales.TotalSales >= 5) , lit("High")) \
     .otherwise(lit("C")) \
  )
display(df_CoffeeSales.head(2))
```

![image](https://github.com/user-attachments/assets/93ccc0d3-15f8-4d6d-a485-895656b35eb5)

```python
df_CoffeeSales.write.format("delta")\
       .mode("overwrite")\
       .saveAsTable("SilverLH.fact_sales")
```

# Product Type Table

```python
# Reading the product type Table from Brinze Lakehouse
df_product_type = spark.read.table("BronzeLH.ProductType")
display(df_product_type.head(2))
```

![image](https://github.com/user-attachments/assets/88728e34-93d4-4c88-9949-ef4119773df5)

```python
# Convert the data type of product type as Integer
df_product_type = df_product_type.withColumn("product_type_id",col("product_type_id").cast(IntegerType()))
```

```python
# Writing the Product Type Table into Silver Lakehouse
df_product_type.write.format("delta")\
       .mode("overwrite")\
       .saveAsTable("SilverLH.dim_product_type")
```

# Import Products Table From Bronze to Silver Lakehouse

```python
# Reading the Products Table from Brinze Lakehouse
df_products = spark.read.table("BronzeLH.Products")
display(df_products.head(2))
```
![image](https://github.com/user-attachments/assets/f078a8f5-6629-4b5f-9e19-5c134b316703)

```python
# Convert the data type of product id colun
df_products = df_products.withColumn("product_id",col("product_id").cast(IntegerType()))\
                         .withColumn("unit_price",col("unit_price").cast(IntegerType()))
```

```python
# Save the Dim products Table into Silver Lakehouse
df_products.write.format("delta")\
       .mode("overwrite")\
       .saveAsTable("SilverLH.dim_products")
```

# Import Stores Table From Bronze to Silver Lakehouse

```python
# Reading the Stores Table from Brinze Lakehouse
df_stores = spark.read.table("BronzeLH.Stores")

display(df_stores)
```

![image](https://github.com/user-attachments/assets/bafffe6a-d613-42a1-b854-9acb1e6eb950)

```python
# Convert Store_Id data type as Integer  
df_stores = df_stores.withColumn("store_id",col("store_id").cast(IntegerType()))
```

```python
# Writing the Stores Table from Brinze to Silver Lakehouse
df_stores.write.format("delta")\
       .mode("overwrite")\
       .saveAsTable("SilverLH.dim_stores")
```



































