# Databricks notebook source
# File uploaded to /FileStore/tables/bookingtype_dim.csv
# File uploaded to /FileStore/tables/bus_type_dim.csv
# File uploaded to /FileStore/tables/city_dim.csv
# File uploaded to /FileStore/tables/cust_dim.csv
# File uploaded to /FileStore/tables/fact_dim.csv
# File uploaded to /FileStore/tables/paymenttype_dim.csv
# File uploaded to /FileStore/tables/seattype_dim.csv

# COMMAND ----------

# spark ke dataframes ke upar

# spark session ka object banate hai

# inferSchema bade data ke lia use nahi karte or time consuming hai, kyunki ye first table se last table tak ka data scan karta hai
fact_df = spark.read.csv("/FileStore/tables/fact_dim.csv",header=True, inferSchema= True)

# COMMAND ----------

fact_df.show()

# COMMAND ----------

fact_df.printSchema()

# COMMAND ----------

# File uploaded to /FileStore/tables/bookingtype_dim.csv
# File uploaded to /FileStore/tables/bus_type_dim.csv
# File uploaded to /FileStore/tables/city_dim.csv
# File uploaded to /FileStore/tables/cust_dim.csv
# File uploaded to /FileStore/tables/fact_dim.csv
# File uploaded to /FileStore/tables/paymenttype_dim.csv
# File uploaded to /FileStore/tables/seattype_dim.csv

cust_df = spark.read.csv("/FileStore/tables/cust_dim.csv",header=True, inferSchema= True)
city_df = spark.read.csv("/FileStore/tables/city_dim.csv",header=True, inferSchema= True)
bustype_df = spark.read.csv("/FileStore/tables/bus_type_dim.csv",header=True, inferSchema= True)
seattype_df = spark.read.csv("/FileStore/tables/seattype_dim.csv",header=True, inferSchema= True)
bookingtype_df = spark.read.csv("/FileStore/tables/bookingtype_dim.csv",header=True, inferSchema= True)
paymenttye_df = spark.read.csv("/FileStore/tables/paymenttype_dim.csv",header=True, inferSchema= True)

# COMMAND ----------

cust_df.show()

# COMMAND ----------

bustype_df.select('Bus_type').show()

# COMMAND ----------

# DBTITLE 1,View creation - Accessible only in the same spark session
# cust_df = spark.read.csv("/FileStore/tables/cust_dim.csv",header=True, inferSchema= True)
# city_df = spark.read.csv("/FileStore/tables/city_dim.csv",header=True, inferSchema= True)
# bustype_df = spark.read.csv("/FileStore/tables/bus_type_dim.csv",header=True, inferSchema= True)
# seattype_df = spark.read.csv("/FileStore/tables/seattype_dim.csv",header=True, inferSchema= True)
# bookingtype_df = spark.read.csv("/FileStore/tables/bookingtype_dim.csv",header=True, inferSchema= True)
# paymenttye_df = spark.read.csv("/FileStore/tables/paymenttype_dim.csv",header=True, inferSchema= True)

bustype_df.createOrReplaceTempView("bus1")
city_df.createOrReplaceTempView("city1")
cust_df.createOrReplaceTempView("cust1")
seattype_df.createOrReplaceTempView("seat1")
bookingtype_df.createOrReplaceTempView("booking1")
fact_df.createOrReplaceTempView("fact1")
paymenttye_df.createOrReplaceTempView("payment1")

# COMMAND ----------

spark.sql('select * from bus1').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * From bus1 

# COMMAND ----------

# MAGIC %sql
# MAGIC select booking_type,bus_type,sum(ticket_fare) as revenue
# MAGIC   from booking1,fact1,bus1
# MAGIC where booking1.booking_id = fact1.booking_id and bus1.type_id = fact1.type_id
# MAGIC group by bus_type,booking_type
# MAGIC order by booking_type,revenue desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select city_from,city_to,ACtype,count(ACType) as seatno
# MAGIC from
# MAGIC (select city_from,city_to,
# MAGIC CASE when seat_type like '%NON AC%' then 'NON AC' else ' AC'  
# MAGIC END  ACtype
# MAGIC from seat1,fact1,city1
# MAGIC where city1.city_id= fact1.city_id and seat1.seat_id=fact1.seat_id)
# MAGIC group by city_from,city_to,ACtype
# MAGIC order by seatno;

# COMMAND ----------

# MAGIC %sql
# MAGIC select city_from,city_to,sum(Ticket_Fare) as revenue,avg(Ticket_fare) as average,max(Ticket_fare) as maximum
# MAGIC from fact1,city1
# MAGIC where city1.city_id=fact1.city_id group by city_from,city_to
# MAGIC order by revenue, average,maximum;
