from kafka import KafkaProducer
import json
import ast


# Get topics list and brokers dict from file
def get_topics_brokers_from_file():
    # Datacenter's name: list of the brokers
    bf = open('brokers_datacenters_list')
    hosts_string = bf.readlines(1)[0]
    bf.close()

    # Magic to transform string representation of a Dictionary to a dictionary
    hosts_dict = ast.literal_eval(hosts_string)
    if not isinstance(hosts_dict, dict):
        raise Exception("В файле какая-то хрень!")

    # List of the topics
    topics_list = []
    tf = open('topics_for_reassign_list')
    for topics in tf:
        topics_list.append(topics[:-1])
    tf.close()

    return topics_list, hosts_dict


# Create json with value for entered topics
def create_dict_for_assign(topics_list, hosts_dict):
    inc = 0

    reassign_info = {
        "partitions": [],
        "version": 1,
    }

    # For each topic:
    for topic in topics_list:
        partitions_list = get_partitions_list(topic)

        # For each partition for this topic:
        for partitions_num in partitions_list:
            inc = inc + 1
            # Add new info to json
            reassign_info["partitions"].append({
                # Write the number of the partition
                'partition': partitions_num,
                # Write the name of the topic without \n
                'topic': topic,
                # Determine brokers for storage partitions for the topic
                'replicas': create_extended_broker_list(hosts_dict, inc),
            })

    return reassign_info


# Get the list of the partitions for topic
def get_partitions_list(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    partitions_list = producer.partitions_for(topic)
    return partitions_list


# Generate new list of the brokers (for storage partitions for each topics on the new datacenters)
def create_extended_broker_list(hosts_dict, inc):
    updated_broker_list = []

    for datacenter_num in hosts_dict:
        # List of the brokers in the current datacenter
        broker_list = hosts_dict[datacenter_num]
        # Choose broker from list
        broker_num = inc % len(broker_list)
        current_broker = broker_list[broker_num]
        updated_broker_list.append(current_broker)

    return updated_broker_list


def main():
    topics_list, hosts_dict = get_topics_brokers_from_file()
    dict_for_json = create_dict_for_assign(topics_list, hosts_dict)
    result_json = json.dumps(dict_for_json)
    # Magic happens here to make it pretty-printed
    pretty_json = json.dumps(json.loads(result_json), indent=4, sort_keys=True)
    print(pretty_json)

    # Now write output to a file
    json_file = open("partitions-to-move.json", "w")
    json_file.write(pretty_json)
    json_file.close()


if __name__ == "__main__":
    main()
