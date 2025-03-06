#!/usr/bin/env python
# coding: utf-8

# ## Transform Retail Data
# 
# New notebook

# In[3]:


# Welcome to your new notebook
# Type here in the cell editor to add code!

df_cust1=spark.read.format("delta").load("Tables/raw_customers_1")

df_cust1.show()


# In[4]:


df_cust2=spark.read.format("delta").load("Tables/raw_customers_2")
df_cust2.show()


# In[12]:


from pyspark.sql.functions import concat_ws

df_cust = df_cust1.union(df_cust2)
df=df_cust.dropDuplicates()
df=df.withColumn("FullName",concat_ws(" ",df["FirstName"],df["LastName"]))


df.write.mode("overwrite").format("delta").save("Tables/prod_customers")


# transform orders

# In[13]:


df_orders=spark.read.format("delta").load("Tables/raw_orders")

df_orders.show()


# In[14]:


from pyspark.sql.functions import col

df_orders=df_orders.withColumn("ProductUnitPrice",df_orders["Total"]/df_orders["Quantity"])
df_orders.show()


# In[15]:


df_orders.write.mode("overwrite").format("delta").save("Tables/prod_orders")

