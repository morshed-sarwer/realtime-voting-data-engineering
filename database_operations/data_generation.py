import requests


BASE_URL= 'https://randomuser.me/api/?nat=us'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

def generate_voter_data():
    try:
        response = requests.get(BASE_URL)
        if(response.status_code == 200):
            user_data = response.json()['results'][0]
            return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
            
        else:
            print("error while fetching data")
            # print(user_data)
        
    except Exception as ex:
        print(ex)


def generate_candidate_data(candidate_number, total_parties):
    try:
        response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
        if(response.status_code==200):
            user_data = response.json()['results'][0]
            
            return{
                "candidate_id": user_data['login']['uuid'],
                "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                "party_affiliation": PARTIES[candidate_number % total_parties],
                "biography": "A brief bio of the candidate.",
                "campaign_platform": "Key campaign promises or platform.",
                "photo_url": user_data['picture']['large']
            }
    except Exception as ex:
        print(ex)
        
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


