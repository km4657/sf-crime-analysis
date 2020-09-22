from kafka import KafkaConsumer

consumer = KafkaConsumer('org.udacity.datastreaming.project2.callevents')

# from https://kafka-python.readthedocs.io/en/master/usage.html
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

