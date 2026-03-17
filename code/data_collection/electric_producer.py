'''
Electric Record Producer
'''

from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import os
import random


# NOTE: Once Airflow is operating we will need to change where current date is stored to make sense in the context of airflow

CONSTANT_RESPONDENTS = ['AEC', 'AECI', 'AVA', 'AVRN', 'AZPS', 'BANC', 'BPAT', 'CAL', 
                        'CAR', 'CENT', 'CHPD', 'CISO', 'CPLE', 'CPLW', 'DEAA', 'DOPD', 
                        'DUK', 'EEI', 'EPE', 'ERCO', 'FLA', 'FMPP', 'FPC', 'FPL', 'GCPD', 
                        'GLHB', 'GRID', 'GRIF', 'GVL', 'GWA',' HGMA', 'HST', 'IID', 'IPCO', 
                        'ISNE', 'JEA', 'LDWP', 'LGEE', 'MIDA', 'MIDW', 'MISO', 'NE', 'NEVP', 
                        'NSB', 'NW', 'NWMT', 'NY', 'NYIS', 'PACE', 'PACW', 'PGE', 'PJM', 
                        'PNM', 'PSCO', 'PSEI', 'SC', 'SCEG', 'SCL', 'SE', 'SEC', 'SEPA', 
                        'SIKE', 'SOCO', 'SPA', 'SRP', 'SW', 'SWPP', 'TAL', 'TEC', 'TEN', 
                        'TEPC', 'TEX', 'TIDC', 'TPWR', 'TVA', 'US48', 'WACM', 'WALC', 'WAUW', 'WWA']


CONSTANT_RESPONDENTS_NAMES = [  'PowerSouth Energy Cooperative', 'Associated Electric Cooperative, Inc.', 'Avista Corporation',
                                'Avangrid Renewables, LLC', 'Arizona Public Service Company', 'Balancing Authority of Northern California',
                                'Bonneville Power Administration', 'California', 'Carolinas', 'Central', 'Public Utility District No. 1 of Chelan County',
                                'California Independent System Operator', 'Duke Energy Progress East', 'Duke Energy Progress West', 'Arlington Valley, LLC',
                                'PUD No. 1 of Douglas County', 'Duke Energy Carolinas', 'Electric Energy, Inc.', 'El Paso Electric Company',
                                'Electric Reliability Council of Texas, Inc.', 'Florida', 'Florida Municipal Power Pool', 'Duke Energy Florida, Inc.', 
                                'Florida Power & Light Co.', 'Public Utility District No. 2 of Grant County, Washington', 'GridLiance', 
                                'Gridforce Energy Management, LLC', 'Griffith Energy, LLC', 'Gainesville Regional Utilities', 'NaturEner Power Watch, LLC', 
                                'New Harquahala Generating Company, LLC', 'City of Homestead', 'Imperial Irrigation District', 'Idaho Power Company', 
                                'ISO New England', 'JEA', 'Los Angeles Department of Water and Power', 
                                'LG&E and KU Services Company as agent for Louisville Gas and Electric Company and Kentucky Utilities Company', 
                                'Mid-Atlantic', 'Midwest', 'Midcontinent Independent System Operator, Inc.', 'New England', 'Nevada Power Company', 
                                'Utilities Commission of New Smyrna Beach', 'Northwest', 'NorthWestern Corporation', 'New York', 'New York Independent System Operator', 
                                'PacifiCorp East', 'PacifiCorp West', 'Portland General Electric Company', 'PJM Interconnection, LLC', 'Public Service Company of New Mexico', 
                                'Public Service Company of Colorado', 'Puget Sound Energy, Inc.', 'South Carolina Public Service Authority', 
                                'Dominion Energy South Carolina, Inc.', 'Seattle City Light', 'Southeast', 'Seminole Electric Cooperative', 
                                'Southeastern Power Administration', 'Sikeston Board of Municipal Utilities', 'Southern Company Services, Inc. - Trans', 
                                'Southwestern Power Administration', 'Salt River Project Agricultural Improvement and Power District', 'Southwest', 
                                'Southwest Power Pool', 'City of Tallahassee', 'Tampa Electric Company', 'Tennessee', 'Tucson Electric Power', 'Texas', 
                                'Turlock Irrigation District', 'City of Tacoma, Department of Public Utilities, Light Division', 'Tennessee Valley Authority', 
                                'United States Lower 48', 'Western Area Power Administration - Rocky Mountain Region', 
                                'Western Area Power Administration - Desert Southwest Region', 'Western Area Power Administration - Upper Great Plains West', 'NaturEner Wind Watch, LLC']

CONSTANT_FUEL_TYPES = ['BAT', 'BAT', 'COL', 'GEO', 'NG', 'NUC', 'OES', 'OIL', 'OTH', 'PS', 'SNB', 'SNB', 'SUN', 'UES', 'UES', 'UNK', 'WAT', 'WNB', 'WND']
CONSTANT_FUEL_TYPE_NAMES = ['Battery', 'Battery storage', 'Coal', 'Geothermal', 'Natural Gas', 'Nuclear', 'Other energy storage', 'Petroleum', 'Other', 'Pumped storage', 
                            'Solar with integrated battery storage', 'Solar Battery', 'Solar', 'Unknown energy storage', 'Unknown Energy', 'Unknown', 'Hydro', 
                            'Wind with integrated battery storage', 'Wind']


def create_producer(bootstrap_servers: str = "kafka:9092"):
    '''
        This function is used to generate the kafka producer, allowing use to add messages to a topic 
    '''
    producer=None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            compression_type="lz4",
            acks="all",
            retries = 5,
            linger_ms=20,
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"    [ERROR] {e}")

    return producer


def generate_records(curr_date: str):
    '''
        This file generates individual records, mocking how the records would come in from the EIA API
    '''
    
    respondent_id = random.randint(0, len(CONSTANT_RESPONDENTS) - 1)
    fuel_id = random.randint(0, len(CONSTANT_FUEL_TYPES) -1)

    record = {
    "period" : curr_date,
    "respondent" : CONSTANT_RESPONDENTS[respondent_id],
    "respondent_name" : CONSTANT_RESPONDENTS_NAMES[respondent_id],
    "fueltype" : CONSTANT_FUEL_TYPES[fuel_id],
    "type-name" : CONSTANT_FUEL_TYPE_NAMES[fuel_id], 
    "value" : random.randint(1, 999),
    "value-units" : "megawatthours"
    }

    return record

def send_records(curr_date: str, topic: str = "electric_records" ):
    '''
        This function makes calls to generate and send generated records to a kafka topic, 
        and also changes the date that should be used next time this is ran
    '''
    producer = create_producer()
    start_time = time.perf_counter()
    try:
        count = 0
        start = time.time()

        while count <= 500:
            #If producer runs the max length allowed it will shut down
            if time.perf_counter() - start_time > 10:
                break


            #Function call generates record
            record = generate_records(curr_date)
            producer.send(
                topic=topic,
                key=record["respondent"],
                value=record
            )

            count += 1
        #Here we are ensuring we keep the time format the same between runs to prevent errors
        new_date = (datetime.strptime(curr_date, "%Y-%m-%dT%H") + timedelta(hours=1)).strftime("%Y-%m-%dT%H")

        with open("record-date.txt", "w") as file:
            file.write(str(new_date))


    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


def main():
    
    #Below a file with a time is created if it doesn't exist, otherwise we retrieve the next date to be read from the file
    curr_date = None
    if not os.path.isfile("record-date.txt"):
        start_date = datetime(2026, 3, 2, 0).strftime("%Y-%m-%dT%H")
        curr_date = start_date
        # print(start_date)
        with open("record-date.txt", "w") as file:
            file.write(str(start_date))
    else:
        with open("record-date.txt", "r") as file:
            curr_date = file.read()
    
    # conn = BaseHook.get_connection

    #Makes sure a date is retieved, if so we start trying to make records
    if curr_date != None:
        send_records(curr_date)






if __name__ == "__main__":
    main()