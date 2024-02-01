import streamlit as st
import time
import psycopg2
from config.settings import DATABASE_PARAMS
from kafka import KafkaConsumer
import simplejson as json


st.title('Welcome to election dashboard')

def create_kafka_consume(topics_name):
    consumer = KafkaConsumer(
        topics_name,
        bootstrap_servers='localhost:9092',
        auto_offsets_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))ytytfyttttttyuuyutytuytty 
    )
    return consumer

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(**DATABASE_PARAMS)
    cur = conn.cursor()
    
    # voters query stats
    voters_query = "SELECT COUNT(*) FROM voters;"
    cur.execute(voters_query)
    voters_count=cur.fetchone()[0]
    
    # candidates query stats
    candidates_query = "SELECT COUNT(*) FROM candidates;"
    cur.execute(candidates_query)
    candidates_count=cur.fetchone()[0]
    
    return voters_count,candidates_count
    
    
def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    voters_count,candidates_count=fetch_voting_stats()
    # print((voters_count,candidates_count))
    
    # display total voters and candidates
    
    st.markdown("___")
    col1,col2 = st.columns(2)
    col1.metric("Total voters",voters_count)
    col2.metric("Total candidates",candidates_count)
    
update_data()