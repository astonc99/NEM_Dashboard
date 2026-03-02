import streamlit as st

st.set_page_config(page_title='NEM Dashboard', layout='wide')
st.title("NEM Victoria Dashboard")

st.write("Hi there! Welcome to this NEM Victoria dashboard - alpha version. Use the sidebar to navigate to the various pages, and feel free to explore the data. The data is updated daily, and you can backfill historical data using the controls on the Prices page.")

st.page_link('pages/1_Prices.py', label='NEM VIC Prices')
st.page_link('pages/2_Generation_Mix.py', label='NEM VIC Generation Mix')