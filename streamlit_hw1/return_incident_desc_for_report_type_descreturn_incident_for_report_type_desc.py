import streamlit as st

from hw1 import *
from user_definition import *


st.title('Incident Description for Report Type')

input = st.text_input('Report Type Description and Number of data to display\
                      (comma separated):')
strings = input.strip().split(',')

if input and len(strings) == 1:
    data = return_incident_desc_for_report_type_desc(
        user, host, dbname,
        strings[0].strip().lower())
    st.table(data)

elif input and len(strings) > 1:
    data = return_incident_desc_for_report_type_desc(
        user, host, dbname,
        strings[0].strip().lower(),
        strings[1].strip())
    st.table(data)
