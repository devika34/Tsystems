import streamlit as st
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sqlalchemy



#sc = SparkContext('local')
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

st.markdown("<h1 style='text-align: center; color: white;'>Data Pipelining Tool</h1>", unsafe_allow_html=True)

@st.cache 
def pipeline(file_loca, file_type):
	
	
	infer_schema = "true"
	first_row_is_header = "true"
	delimiter = ","
	
	if file_type!="xlsx":
		df=spark.read.format(file_type) \
    	.option("inferSchema", infer_schema) \
    	.option("header", first_row_is_header) \
    	.option("sep", delimiter) \
    	.option('nanValue', ' ')\
    	.option('nullValue', ' ')\
    	.load(file_loca)
		df = df.toPandas()
	else:
		df=pd.read_excel(file_loca)


	
	
	return df


multiple_files = st.file_uploader('' ,accept_multiple_files=True)
for file in multiple_files:
	st.write(file.name)
	#st.write(file.name.rpartition('.')[0])
	can=file.name.rpartition('.')[0]
	dataframe = pipeline(file.name,file.name.rpartition('.')[2])
	file.seek(0)
	st.dataframe(dataframe)


st.write(" ")
st.write(" ")

#user_input = st.text_input("What do you want to name the database?")

text_input=''
submit_button=''
db=''

with st.form(key='my_form'):
	text_input = st.text_input(label='Which database do you wish to choose?')
	submit_button = st.form_submit_button(label='Create Database')
	#st.write(submit_button)
	db='sqlite:///'+text_input+'.db'
	#st.write(db)


if submit_button==True:

	if text_input=='':
		st.error('Database field cannot be empty')
		
	else:
		database = sqlalchemy.create_engine(db)
		for file in multiple_files:
			#st.write(file.name)
			#st.write(file.name.rpartition('.')[0])
			dataframe = pipeline(file.name,file.name.rpartition('.')[2])
			#file.seek(0)
			dataframe.to_sql(file.name.rpartition('.')[0], db, if_exists="replace")
		
	if text_input!='':	
		st.success('Successfully created database '+text_input+'! please refresh the page')
	
	

	



hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)









if submit_button==True:
    if text_input=='':
		st.error('Database field cannot be empty')
    else:
        work()


st.success('Successfully created database '+text_input+'! please refresh the page')




''' 
c.execute(''''''CREATE TABLE check (Type text)'''''')


# load the data into a Pandas DataFrame
users = pd.read_csv('DataCoSupplyChainData.csv', encoding = 'unicode_escape')
# write the data to a sqlite table
users.to_sql('check', conn, if_exists='append', index = False)
c.execute(''''''SELECT * FROM check'''''').fetchall()
'''

'''options=csv_to_sqlite.CsvOptions(typing_style="full",encoding="cp1252")
csv_to_sqlite.write_csv("DataCoSupplyChainData.csv", "output.sqlite", options)

con=sqlite3.connect('output.sqlite')
filter_data=pd.read_sql_query("""SELECT * FROM output.sqlite""",con)'''

'''Days for shipping "\("real"\)" text, Days for shipment "\("scheduled"\)" text, Benefit per order text, Sales per customer text, Delivery Status text, Late_delivery_risk text, Category Id text, Category Name text, Customer City text, Customer Country text, Customer Email text, Customer Fname text, Customer Id text, Customer Lname text, Customer Password text, Customer Segment text, Customer State text, Customer Street text, Customer Zipcode text, Department Id text, Department Name text, Latitude text, Longitude text, Market text, Order City text, Order Country text, Order Customer Id text, order date (DateOrders) text, Order Id text, Order Item Cardprod Id text, Order Item Discount text, Order Item Discount Rate text, Order Item Id text, Order Item Product Price text, Order Item Profit Ratio text, Order Item Quantity text, Sales text, Order Item Total text, Order Profit Per Order text, Order Region text, Order State text, Order Status text, Order Zipcode text, Product Card Id text, Product Category Id text, Product Description text, Product Image text, Product Name text, Product Price text, Product Status text, shipping date (DateOrders) text, Shipping Mode text '''


[Customer Email]
[Customer Password]
[Order Zipcode]
[Product Description]
[Product Image]

    insert_records = "INSERT INTO hello ([Type], [Days for shipping (real)], [Days for shipment (scheduled)], [Benefit per order], [Sales per customer], [Delivery Status], [Late_delivery_risk], [Category Id], [Category Name], [Customer City], [Customer Country], [Customer Email], [Customer Fname], [Customer Id], [Customer Lname], [Customer Password], [Customer Segment], [Customer State], [Customer Street], [Customer Zipcode], [Department Id], [Department Name], [Latitude], [Longitude], [Market], [Order City], [Order Country], [Order Customer Id], [order date (DateOrders)], [Order Id], [Order Item Cardprod Id], [Order Item Discount], [Order Item Discount Rate], [Order Item Id], [Order Item Product Price], [Order Item Profit Ratio], [Order Item Quantity], [Sales], [Order Item Total], [Order Profit Per Order], [Order Region], [Order State], [Order Status], [Order Zipcode], [Product Card Id], [Product Category Id], [Product Description], [Product Image], [Product Name], [Product Price], [Product Status], [shipping date (DateOrders)], [Shipping Mode]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
 create_table = '''CREATE TABLE hello(
            [Type] text, [Days for shipping (real)]  text, [Days for shipment (scheduled)] text, [Benefit per order] text, [Sales per customer] text, [Delivery Status] text, [Late_delivery_risk] text, [Category Id] text, [Category Name] text, [Customer City] text, [Customer Country] text, [Customer Email] text, [Customer Fname] text, [Customer Id] text, [Customer Lname] text, [Customer Password] text, [Customer Segment] text, [Customer State] text, [Customer Street] text, [Customer Zipcode] text, [Department Id] text, [Department Name] text, [Latitude] text, [Longitude] text, [Market] text, [Order City] text, [Order Country] text, [Order Customer Id] text, [order date (DateOrders)] text, [Order Id] text, [Order Item Cardprod Id] text, [Order Item Discount] text, [Order Item Discount Rate] text, [Order Item Id] text, [Order Item Product Price] text, [Order Item Profit Ratio] text, [Order Item Quantity] text, [Sales] text, [Order Item Total] text, [Order Profit Per Order] text, [Order Region] text, [Order State] text, [Order Status] text, [Order Zipcode] text, [Product Card Id] text, [Product Category Id] text, [Product Description] text, [Product Image] text, [Product Name] text, [Product Price] text, [Product Status] text, [shipping date (DateOrders)] text, [Shipping Mode] text);
            '''

			

