
import boto3
import botocore
import json


class S3ObjectReader:
    aws_access_key_id = ''
    aws_secret_access_key = ''
    region_name = ''
    bucket = ''
    prefix = ''
    record_delimeter = ''
    s3 = ''

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, bucket, prefix, record_delimeter):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.bucket = bucket
        self.prefix = prefix
        self.record_delimeter = record_delimeter
        self.s3 = boto3.client('s3',
                        aws_access_key_id= self.aws_access_key_id, 
                        aws_secret_access_key= self.aws_secret_access_key, 
                        region_name=self.region_name)

       

    def __get_files_name_in_bucket(self):
        files = []
        responce = self.s3.list_objects_v2(Bucket = self.bucket, Prefix = self.prefix)
        for obj in responce['Contents']:
            files.append(obj['Key'])
        return files
    
    def __extract_content_for_each_file(self):
        files = self.__get_files_name_in_bucket()
        content = {}
        for file in files:
            partitioned_key = file.split('/')[1]
            content[partitioned_key] = self.s3.select_object_content(Bucket = self.bucket, Key = file,
                                        InputSerialization = {'Parquet': {}}, OutputSerialization = {'JSON': {'RecordDelimiter': self.record_delimeter}},
                                        ExpressionType = 'SQL',
                                        Expression = """select * from s3object s""")
        return content
    
    def extract_and_convert_events_to_objects(self):
        content = self.__extract_content_for_each_file()
        events_object = []
        for partitioned_key in content.keys():
            for event in content[partitioned_key]['Payload']:
                        if 'Records' in event:
                            json_record = event['Records']['Payload'].decode('utf-8')
                            records_split = json_record.split('\n')
                            for r in records_split:
                                if r != "":
                                    events_object.append(EventData.create_from_json(r, partitioned_key))

        return events_object

class EventData:
    def __init__(self, device, ip_address, impressions, ad_blocking_rate):
        self.device = device
        self.ip_address = ip_address
        self.impressions = impressions
        self.ad_blocking_rate = ad_blocking_rate
        self.partitioned_key = 0

    @staticmethod
    def create_from_json(data, partitioned_key):
        json_dictionary = json.loads(data)
        ed = EventData(**json_dictionary)
        ed.partitioned_key = partitioned_key
        return ed

    def __str__(self):
        return 'Event:' + 'Partitioned_key: ' + self.partitioned_key + '(Device: ' + self.device + ' Ip_address: ' + self.ip_address + ' Impressions: ' + str(self.impressions) + ' Ad_blocking_rate: ' + str(self.ad_blocking_rate)


def main():
    s3_object_reader = S3ObjectReader(aws_access_key_id='AKIAQ47YXDJQGLTHJVG7',
        aws_secret_access_key='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',
        region_name='us-east-1', bucket='bdc21-yuriy-danko-user-bucket',
        prefix='incoming-data-parquet', record_delimeter= "\n")
    try:
        events = s3_object_reader.extract_and_convert_events_to_objects()
    except botocore.exceptions.ParamValidationError as e:
        print(str(e))
        return
    except botocore.exceptions.ClientError as e:
        print(str(e))
        return
    except Exception as e:
        print(str(e))
        return

    for event in events:
        print(event)

if __name__ == "__main__":
    main()



