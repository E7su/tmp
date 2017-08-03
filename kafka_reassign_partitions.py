from kafka import KafkaProducer
import json
import ast


def get_topics_brokers_from_file():
    """Get topics list and brokers dict from file

    Return:
    topics_list - list with topics
    brokers_dict - dict with brokers distributed by data centers {datacenter: [brokers]}
                                                             {1: [0, 1, 2, 3],
                                                              2: [4, 5],
                                                              3: [6, 7, 8],}

    """
    # Datacenter's name: list of the brokers
    bf = open('brokers_datacenters_list')
    brokers_string = bf.readlines(1)[0]
    bf.close()

    # Magic to transform string representation of a Dictionary to a dictionary
    brokers_dict = ast.literal_eval(brokers_string)
    if not isinstance(brokers_dict, dict):
        raise Exception('The line in the file "brokers_datacenters_list" does not match the dictionary format')

    # List of the topics
    topics_list = []
    tf = open('topics_for_reassign_list')
    for topics in tf:
        topics_list.append(topics[:-1])
    tf.close()

    return topics_list, brokers_dict


def create_dict_for_assign(topics_list, brokers_dict):
    """Get topics list and brokers dict from file

    Arguments:
    topics_list - list with topics
    brokers_dict - dict with brokers distributed by data centers {datacenter: [brokers]}
                                                             {1: [0, 1, 2, 3],
                                                              2: [4, 5],
                                                              3: [6, 7, 8],}

    Return:
    reassign_info - dict for json for reassign command

    """
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
                'replicas': create_extended_broker_list(brokers_dict, inc),
            })

    return reassign_info


def get_partitions_list(topic):
    """Get the list of the partitions for topic

    Arguments:
    topic - name of the topic
    
    Return:
    partitions_list - list of the topic's partitions

    """
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    partitions_list = producer.partitions_for(topic)
    return partitions_list


def create_extended_broker_list(brokers_dict, inc):
    """Generate new list of the brokers 
                (for storage partitions for each topics on the new datacenters)

    Arguments:
    brokers_dict - dict with brokers distributed by data centers 
                                                {datacenter: [brokers]}
                                                Example:
                                                {1: [0, 1, 2, 3],
                                                 2: [4, 5],
                                                 3: [6, 7, 8],}
    inc - counter of the partitions                                                              

    Return:
    updated_broker_list - list with new brokers on which 
                                     the partitions will be kept after reassign

    """
    updated_broker_list = []

    for datacenter_num in brokers_dict:
        # List of the brokers in the current datacenter
        broker_list = brokers_dict[datacenter_num]
        # Choose broker from list
        broker_num = inc % len(broker_list)
        current_broker = broker_list[broker_num]
        updated_broker_list.append(current_broker)

    return updated_broker_list


def main():
    topics_list, brokers_dict = get_topics_brokers_from_file()
    dict_for_json = create_dict_for_assign(topics_list, brokers_dict)
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
