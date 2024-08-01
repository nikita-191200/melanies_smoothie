import streamlit as st
from snowflake.connector import connect
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import requests


fruityvice_response = requests.get("https://fruityvice.com/api/fruit/watermelon")
st.text(fruityvice_response)

# Write directly to the app
st.title("Example Streamlit App :balloon:")
st.write(
    """
    Choose your Fruits!
    """
)
name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

# Retrieve Snowflake connection details from Streamlit secrets
snowflake_secrets = st.secrets["snowflake"]

# Establish a connection to Snowflake using Snowpark session
conn = connect(
    user=snowflake_secrets["user"],
    password=snowflake_secrets["password"],
    account=snowflake_secrets["account"],
    role=snowflake_secrets["role"],
    warehouse=snowflake_secrets["warehouse"],
    database=snowflake_secrets["database"],
    schema=snowflake_secrets["schema"],
    client_session_keep_alive=snowflake_secrets["client_session_keep_alive"]
)

# Initialize Snowpark session
session = Session.builder.configs({
    "account": snowflake_secrets["account"],
    "user": snowflake_secrets["user"],
    "password": snowflake_secrets["password"],
    "role": snowflake_secrets["role"],
    "warehouse": snowflake_secrets["warehouse"],
    "database": snowflake_secrets["database"],
    "schema": snowflake_secrets["schema"]
}).create()

query = "SELECT FRUIT_NAME FROM SMOOTHIES.PUBLIC.FRUIT_OPTIONS"
fruit_options_df = pd.read_sql(query, conn)

my_dataframe = session.table('SMOOTHIES.PUBLIC.FRUIT_OPTIONS').select(col('FRUIT_NAME'), col('SEARCH_ON'))

# st.dataframe(data=my_dataframe.collect(), use_container_width=True)
pd_df = my_dataframe.to_pandas()
# st.dataframe(pd_df)
# st.stop()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    # fruit_options_df['FRUIT_NAME'].tolist(),
    my_dataframe,
    max_selections=5
)

if ingredients_list:
    # ingredients_string = ', '.join(ingredients_list)
    ingredients_string = ''

    insert_stmt = f"""
    INSERT INTO SMOOTHIES.PUBLIC.ORDERS (ingredients, name_on_order)
    VALUES (%s, %s)
    """

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')
        
        st.subheader(f'{fruit_chosen} Nutrition Information')
        fruityvice_url = f"https://fruityvice.com/api/fruit/{fruit_chosen}"
        st.write(f"Requesting URL: {fruityvice_url}")

        fruityvice_response = requests.get(fruityvice_url)

        if fruityvice_response.status_code == 200:
            response_json = fruityvice_response.json()
            
            fv_df = pd.json_normalize(response_json)
            
            st.dataframe(fv_df, use_container_width=True)
            
        elif fruityvice_response.status_code == 404:
            st.error(f"No data found for {fruit_chosen}. Please check the fruit name.")
        else:
            st.error(f"Error fetching data: {fruityvice_response.status_code}")
        
    
    if st.button('Submit Order'):
        conn.cursor().execute(insert_stmt, (ingredients_string, name_on_order))
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

conn.close()
session.close()
