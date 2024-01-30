from datetime import datetime

def create_table(cur):
    
    cur.execute("""
               
               CREATE TABLE IF NOT EXISTS spark_table (
                    id SERIAL PRIMARY KEY,
                    voter_id VARCHAR(255) NOT NULL,
                    voter_name VARCHAR(255) NOT NULL,
                    date_of_birth VARCHAR(255) NOT NULL
                    )
               """)
    
    cur.execute(
    """
    CREATE TABLE IF NOT EXISTS candidates(
        candidate_id VARCHAR(255) PRIMARY KEY,
        candidate_name VARCHAR(255),
        party_affiliation VARCHAR(255),
        biography TEXT,
        campaign_platform TEXT,
        photo_url TEXT
    )
    """
    )
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)



def insert_vote(cur,voter_id, candidate_id,conn):
    vote = 1
    voting_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    cur.execute("""
                INSERT INTO votes (voter_id, candidate_id, voting_time)
                VALUES (%s, %s, %s)
                """,
                (voter_id, candidate_id,voting_time))
    
    conn.commit()
def insert_voters(cur,voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    
def insert_candidates(cur,candidate):
    cur.execute("""
                
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                VALUES(%s,%s,%s,%s,%s,%s)

                """, (candidate['candidate_id'],candidate['candidate_name'],candidate['party_affiliation'],candidate['biography'],candidate['campaign_platform'],candidate['photo_url']))