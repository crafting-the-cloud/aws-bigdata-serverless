import boto3
from pprint import pprint
import json
def main():
    profile_name = "aws_admin"
    region_name = 'us-east-1'
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    client = session.client('firehose')
    records = []

    with open("engine_records_n.json") as file:
        rpms = json.load(file)
        count = 1
       
        for rpm in rpms:
            if count % 500 == 0:
              
              response = client.put_record_batch(
                    DeliveryStreamName='rpm-data',
                    Records=records
                )
              print(response)
              print(len(records))
              records.clear()
            record = {
                "Data": json.dumps(rpm)
            }
            records.append(record)
            count = count + 1

        if len(records) > 0:
            
            print(len(records))
            response = client.put_record_batch(
                DeliveryStreamName='rpm-data',
                Records=records
            )
            print(response)


if __name__=='__main__':
    main()
