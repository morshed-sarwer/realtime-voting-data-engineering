import datetime
import random
import time
from config.settings import DATABASE_PARAMS, voter_topics,KAFKA_BROKER
import psycopg2
from confluent_kafka import Consumer, KafkaError, SerializingProducer
import simplejson as json
from database_operations.data_generation import delivery_report
from database_operations.database_setup import insert_vote


consumer = Consumer( KAFKA_BROKER | {
  'group.id':'voting_group',
  'auto.offset.reset':'earliest',
  'enable.auto.commit':False  
})

producer = SerializingProducer(KAFKA_BROKER)
if __name__ == "__main__":

    conn = psycopg2.connect(**DATABASE_PARAMS)
    cur = conn.cursor()
    query = """
    SELECT row_to_json(can)
    from (SELECT * FROM candidates) can
    """
    candidates_query = cur.execute(query)
    candidates=cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    # print(candidates)
    
    
    # creating subcriber
    consumer.subscribe([voter_topics])
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            elif message.error():
                if message.error().code()==KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error form ${message.error()}")
                    break
                
            else:
                voter_data = json.loads(message.value().decode("utf-8"))
                # print(voter_data)
                chosen_candidate = random.choice(candidates)
                # print(chosen_candidate)
                vote = chosen_candidate | voter_data | {
                    'voting_time': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }
                
                try:
                    insert_vote(cur,voter_id=vote['voter_id'],candidate_id=vote['candidate_id'],conn=conn)
                    producer.produce(
                        topic='vote_topics',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    # print(vote)
                except Exception as e:
                    print("Error while inserting vote: " + str(e))
            time.sleep(0.5)
    except KafkaError as e:
        print("Error")