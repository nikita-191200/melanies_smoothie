# Import python packages
import streamlit as st
import requests
import pandas as pd
from snowflake.snowpark.functions import col


# Write directly to the app
st.title("Customize your Smoothie!! :balloon:")
st.write(
"""
This is test!!!
""")

cnx=st.connection('snowflake')
session = cnx.session()

name_on_order = st.text_input('Name on Smoothie: ')

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

pd_df=my_dataframe.to_pandas()


ingredient_list = st.multiselect(
    'Choose upto 5 ingradients: ', my_dataframe
    , max_selections=5
    )

if ingredient_list:
    ingredient_string = ''

    for fruit_chosen in ingredient_list:
        ingredient_string += fruit_chosen + ' '

        search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')
        
        st.subheader(fruit_chosen+ ' : Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+ fruit_chosen)
        fv_df=st.dataframe(data=fruityvice_response.json(), use_container_width=True)
        
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order) values ('""" + ingredient_string + """', '""" + name_on_order + """')"""
 
    time_to_submit = st.button('Submit Order', type="primary")

    if time_to_submit:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered! ' +name_on_order, icon="✅")
