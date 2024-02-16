import streamlit as st

from hw1 import *
from user_definition import *


st.title('Neighborhood and Police District')

n = st.text_input('Number of data to display:')

if n:
    data = return_distinct_neighborhood_police_district(user, host, dbname, n)
    st.table(data)

else:
    data = return_distinct_neighborhood_police_district(user, host, dbname)
    st.table(data)
