'''
This file is used to generate the kafka topics needed

'''


from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_admin_client(bootstrap_servers: str = "localhost:9092, localhost:9093"):
    '''
    This function is used to create an admin client
    '''

    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="electric-admin"
    )

def create_topic(admin_client: KafkaAdminClient, topic_name: str, partitions: int, retention_days: int):
    '''
    This function is used to generate a new topic
    '''

    retention_ms = retention_days * 1000 * 60 * 60 * 24

    electric_topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=1,
        topic_configs={"retention.ms": str(retention_ms)}
    )

    if electric_topic == None: 
        print("Insert a valid topic!")
        return False


    try:
        admin_client.create_topics([electric_topic])
    except TopicAlreadyExistsError:
        print(f"    [INFO] Topic already exists")
    except Exception as e:
        print(f"    [ERROR] {e}")
        return False
    
    return True








def main():
    
    admin = create_admin_client()

    create_topic(admin, "electric_records", 4, 14)



if __name__ == "__main__":
    main()