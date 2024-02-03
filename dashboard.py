import streamlit as st
import time
import psycopg2
from config.settings import DATABASE_PARAMS
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd


st.title('Welcome to election dashboard')

def create_kafka_consumer(topics_name):
    consumer = KafkaConsumer(
        topics_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def get_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

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
    
    
    # display total voters and candidates metrics
    
    topics_name = "votes_per_candidate"
    consumer = create_kafka_consumer(topics_name)
    kafka_data = get_data_from_kafka(consumer)
    
    results = pd.DataFrame(kafka_data)
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # st.dataframe(df)
    # st.dataframe(result)
    
    # Display leading candidate information
    st.markdown("""---""")
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['picture'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))
    
    
    
update_data()