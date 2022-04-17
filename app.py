from colorama import Cursor
import sqlite3
import pandas as pd
import csv
import streamlit as st
import numpy as np # linear algebra
import seaborn as sns; sns.set(style="ticks", color_codes=True)
import matplotlib.pyplot as plt
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import seaborn as sns
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected = True)



def work():
    from pathlib import Path
    Path('my_data.db').touch()
    conn = sqlite3.connect('my_data.db')
    c = conn.cursor()
    create_table = '''CREATE TABLE hello(
            [Type] text, [Days for shipping (real)]  text, [Days for shipment (scheduled)] text, [Benefit per order] text, [Sales per customer] text, [Delivery Status] text, [Late_delivery_risk] text, [Category Id] text, [Category Name] text, [Customer City] text, [Customer Country] text, [Customer Email] text, [Customer Fname] text, [Customer Id] text, [Customer Lname] text, [Customer Password] text, [Customer Segment] text, [Customer State] text, [Customer Street] text, [Customer Zipcode] text, [Department Id] text, [Department Name] text, [Latitude] text, [Longitude] text, [Market] text, [Order City] text, [Order Country] text, [Order Customer Id] text, [order date (DateOrders)] text, [Order Id] text, [Order Item Cardprod Id] text, [Order Item Discount] text, [Order Item Discount Rate] text, [Order Item Id] text, [Order Item Product Price] text, [Order Item Profit Ratio] text, [Order Item Quantity] text, [Sales] text, [Order Item Total] text, [Order Profit Per Order] text, [Order Region] text, [Order State] text, [Order Status] text, [Order Zipcode] text, [Product Card Id] text, [Product Category Id] text, [Product Description] text, [Product Image] text, [Product Name] text, [Product Price] text, [Product Status] text, [shipping date (DateOrders)] text, [Shipping Mode] text);
            '''
    conn.execute(create_table)
    file = open('DataCoSupplyChainData.csv', encoding = 'unicode_escape' )
    contents = list(csv.reader(file))
    contents1 = []
    
    for i in range(100):
        contents1.append(contents[i])
    
    insert_records = "INSERT INTO hello ([Type], [Days for shipping (real)], [Days for shipment (scheduled)], [Benefit per order], [Sales per customer], [Delivery Status], [Late_delivery_risk], [Category Id], [Category Name], [Customer City], [Customer Country], [Customer Email], [Customer Fname], [Customer Id], [Customer Lname], [Customer Password], [Customer Segment], [Customer State], [Customer Street], [Customer Zipcode], [Department Id], [Department Name], [Latitude], [Longitude], [Market], [Order City], [Order Country], [Order Customer Id], [order date (DateOrders)], [Order Id], [Order Item Cardprod Id], [Order Item Discount], [Order Item Discount Rate], [Order Item Id], [Order Item Product Price], [Order Item Profit Ratio], [Order Item Quantity], [Sales], [Order Item Total], [Order Profit Per Order], [Order Region], [Order State], [Order Status], [Order Zipcode], [Product Card Id], [Product Category Id], [Product Description], [Product Image], [Product Name], [Product Price], [Product Status], [shipping date (DateOrders)], [Shipping Mode]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    # Importing the contents of the file
    # into our person table
    conn.executemany(insert_records, contents1)
    select_all = text_input#"SELECT * FROM hello"
    rows = conn.execute(select_all).fetchall()
    for i in range(10):
        st.write(rows[i])
    st.success('Successfully displayed first 10 rows of the table')


st.markdown("<h1 style='text-align: center; color: white;'>Data Supply Chain Data</h1>", unsafe_allow_html=True)


st.write(" ")
st.write(" ")

df = pd.read_csv("DataCoSupplyChainData1.csv", encoding = 'unicode_escape')

st.write("Delivery Status vs Number of Orders")
data_delivery_status=df.groupby(['Delivery Status'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= False)
fig = px.bar(x=data_delivery_status['Delivery Status'] , y=data_delivery_status['Number of Orders']  , color=data_delivery_status['Number of Orders'],
      labels = { 'Delivery Status': 'Delivery Status', 'Number of Orders': 'Number of Orders'})
st.plotly_chart(fig, use_container_width=True)


data_delivery_status_region=df.groupby(['Delivery Status', 'Order Region'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= False)
fig = px.bar(data_delivery_status_region, x='Delivery Status', y='Number of Orders'  , color='Order Region',)
st.plotly_chart(fig, use_container_width=True)

st.write("Customers according to the Number of Orders")
df['Customer_ID_STR']=df['Customer Id'].astype(str)

data_customers=df.groupby(['Customer_ID_STR'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= False)
fig =px.bar(data_customers.head(20),x='Number of Orders', y='Customer_ID_STR' , color='Number of Orders'      )

st.plotly_chart(fig, use_container_width=True)

st.write("Customers according to the Profit Earned")

df['Customer_ID_STR']=df['Customer Id'].astype(str)

data_customers_profit=df.groupby(['Customer_ID_STR'])['Order Profit Per Order'].sum().reset_index(name='Profit of Orders').sort_values(by= 'Profit of Orders', ascending= False)
fig =px.bar(data_customers_profit.head(20),x='Profit of Orders', y='Customer_ID_STR' , color='Profit of Orders'      )
st.plotly_chart(fig, use_container_width=True)

data_Customer_Segment=df.groupby(['Customer Segment'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= False)
fig = px.pie(data_Customer_Segment, values='Number of Orders', names= 'Customer Segment' , title= 'Number of Orders of different Customer Segments', 
       width=600 , height=600 , color_discrete_sequence = px.colors.sequential.RdBu)

st.plotly_chart(fig, use_container_width=True)

st.write("Number of Orders vs Category")

data_Category_Name=df.groupby(['Category Name'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= True)
fig = px.bar(data_Category_Name, x='Number of Orders',y = 'Category Name',color ='Number of Orders')
st.plotly_chart(fig, use_container_width=True)

st.write("Number of Orders vs Region")
data_Region=df.groupby(['Order Region'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= True)
fig =px.bar(data_Region, x='Number of Orders',y = 'Order Region',color ='Number of Orders')
st.plotly_chart(fig, use_container_width=True)


st.write("Number of Orders vs Country")
data_countries=df.groupby(['Order Country'])['Order Id'].count().reset_index(name='Number of Orders').sort_values(by= 'Number of Orders', ascending= True)
fig =px.bar(data_countries.head(20), x='Number of Orders',y = 'Order Country',color ='Number of Orders')
st.plotly_chart(fig, use_container_width=True)


st.write("Number of Orders vs Geographical Features")
df_geo=df.groupby([ 'Order Country', 'Order City'])['Order Profit Per Order'].sum().reset_index(name='Profit of Orders').sort_values(by= 'Profit of Orders', ascending= False)
fig = px.choropleth(df_geo ,  locationmode='country names', locations='Order Country',
                    color='Profit of Orders', # lifeExp is a column of data
                    hover_name='Order Country', 
                    #hover_data ='Order City',
                    color_continuous_scale=px.colors.sequential.Plasma)
st.plotly_chart(fig, use_container_width=True)

st.write("Analysis of Sales")

df_sales_country=df.groupby([ 'Order Country'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_country.head(10), x='Sales of Orders',y = 'Order Country',color ='Sales of Orders')
st.plotly_chart(fig, use_container_width=True)


df_sales_country=df.groupby([ 'Product Name'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig= px.bar(df_sales_country.head(10), x='Sales of Orders',y = 'Product Name',color ='Sales of Orders')
st.plotly_chart(fig, use_container_width=True)

df_sales_pd=df.groupby([ 'Product Name', 'Delivery Status'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_pd.head(10), x='Sales of Orders',y = 'Product Name',color ='Delivery Status')
st.plotly_chart(fig, use_container_width=True)

df_sales_pr=df.groupby([ 'Product Name', 'Order Region'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_pr.head(10), x='Sales of Orders',y = 'Product Name',color ='Order Region')
st.plotly_chart(fig, use_container_width=True)


df_sales_pr=df.groupby([  'Category Name'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_pr.head(10), x='Sales of Orders',y = 'Category Name',color ='Sales of Orders')
st.plotly_chart(fig, use_container_width=True)


df_sales_pr=df.groupby([ 'Type'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_pr.head(10), x='Sales of Orders',y = 'Type',color ='Sales of Orders')
st.plotly_chart(fig, use_container_width=True)


df_sales_tp=df.groupby([ 'Type', 'Product Name'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig=px.bar(df_sales_tp.head(10), x='Sales of Orders',y = 'Type',color ='Product Name')
st.plotly_chart(fig, use_container_width=True)



import datetime as dt

st.write("Analysis of Sales and Date")

data_orderdate=df[['order date (DateOrders)', 'Sales']]
data_orderdate['order_date'] = pd.to_datetime(data_orderdate['order date (DateOrders)'])

data_orderdate["Quarter"] = data_orderdate['order_date'].dt.quarter
data_orderdate["Month"] = data_orderdate['order_date'].dt.month
data_orderdate["year"] = data_orderdate['order_date'].dt.year

data_orderdate['YearStr']=data_orderdate['year'].astype(str)
df_sales_year=data_orderdate.groupby([ 'YearStr'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_year, x='Sales of Orders',y = 'YearStr',color ='Sales of Orders')
st.plotly_chart(fig, use_container_width=True)

data_orderdate['QuarterStr']=data_orderdate['Quarter'].astype(str)
df_sales_quarter=data_orderdate.groupby([ 'YearStr','QuarterStr'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_quarter, x='Sales of Orders',y = 'QuarterStr',color ='YearStr')
st.plotly_chart(fig, use_container_width=True)


data_orderdate['MonthStr']=data_orderdate['Month'].astype(str)
df_sales_m=data_orderdate.groupby([ 'QuarterStr', 'MonthStr'])['Sales'].sum().reset_index(name='Sales of Orders').sort_values(by= 'Sales of Orders', ascending= False)
fig =px.bar(df_sales_m, x='Sales of Orders',y = 'QuarterStr',color ='MonthStr')
st.plotly_chart(fig, use_container_width=True)









text_input=""
submit_button=''

with st.form(key='my_form'):
	text_input = st.text_input(label='Input the query you want the output for?')
	submit_button = st.form_submit_button(label='Run Query')
	#st.write(submit_button)
	
	#st.write(db)

if text_input!='':
    work()	
	
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)




