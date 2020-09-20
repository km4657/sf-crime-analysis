from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        f = open(self.input_file) 
        data = json.load(f) 
        for i in data:
            message = self.dict_to_binary(i)
            print(message)
            self.send(self.topic, message )
            print('after sending message')
            time.sleep(1)
        f.close() 
    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8') 
    