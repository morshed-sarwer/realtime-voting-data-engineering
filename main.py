
import psycopg2
from config.settings import DATABASE_PARAMS, KAFKA_BROKER,voter_topics
from database_operations.database_setup import create_table,insert_candidates,insert_voters
from database_operations.data_generation import generate_candidate_data,generate_voter_data,delivery_report
from confluent_kafka import SerializingProducer
import simplejson as json


if __name__ == "__main__":
    conn = psycopg2.connect(**DATABASE_PARAMS)
    conn.autocommit=True
    
    producer = SerializingProducer(KAFKA_BROKER)
    cur = conn.cursor()
    # creating tables: candidates, votes, voters
    create_table(cur=cur)
    
    # inserting into candidate table
    cur.execute("""
                SELECT * FROM candidates
                """)
    candidates=cur.fetchall()
    if len(candidates)==0:
        for i in range(3):
            candidate = generate_candidate_data(i,3)
            # print(candidate)
            insert_candidates(cur,candidate=candidate)
    for i in range(500):
        voter_data = generate_voter_data()
        print(voter_data)
        insert_voters(cur=cur, voter=voter_data)
        
        # to see topics Exec command-> kafka-topics --list --bootstrap-server broker:29092
        # to see kafka data Exec command-> kafka-console-consumer --topic voter_topic --bootstrap-server broker:29092
        producer.produce(voter_topics,
                         key=voter_data['voter_id'],
                         value=json.dumps(voter_data),
                         on_delivery=delivery_report
                         )
        
        print('Produced voter {}, data: {}'.format(i, voter_data))
        
        producer.flush()