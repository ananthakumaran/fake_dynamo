# FakeDynamo [![Build Status](https://secure.travis-ci.org/ananthakumaran/fake_dynamo.png)](http://travis-ci.org/ananthakumaran/fake_dynamo)

local hosted, inmemory Amazon DynamoDB emulator.

## Versions

| Amazon DynamoDB API version | FakeDynamo gem version|
| --------------------------- | ----------------------|
| [2012-08-10][v2]            | 0.2.5                 |
| [2011-12-05][v1]            | 0.1.3                 |


## Caveats

*  `ConsumedCapacityUnits` value will be 1 always.

## Usage

__requires ruby >= 1.9__

````
gem install fake_dynamo --version 0.2.5

fake_dynamo --port 4567
````

send a DELETE request to reset the database. eg

````
curl -X DELETE http://localhost:4567
````

## Clients

* [aws-sdk-ruby](https://github.com/aws/aws-sdk-ruby) (AWS SDK for Ruby)

````ruby
AWS.config(:use_ssl => false,
           :dynamo_db_endpoint => 'localhost',
           :dynamo_db_port => 4567,
           :access_key_id => "xxx",
           :secret_access_key => "xxx")
````

* [aws-sdk-js](https://github.com/aws/aws-sdk-js) (AWS SDK for Node.js)

````js
 AWS.config.update({apiVersion:      "2012-08-10",
                    sslEnabled:      false,
                    endpoint:        "localhost:4567",
                    accessKeyId:     "xxx",
                    secretAccessKey: "xxx",
                    region:          "xxx"});
````

* [aws-sdk-java](https://github.com/aws/aws-sdk-java) (AWS SDK for Java)

````java
AWSCredentials credentials = new BasicAWSCredentials("xxx", "xxx");
AmazonDynamoDB client = new AmazonDynamoDBClient(credentials);
client.setEndpoint("http://localhost:4567");
````

__please open a pull request with your configuration if you are using
fake_dynamo with clients other than the ones mentioned above__.

## Storage
fake_dynamo stores the `write operations` (request that changes the
data) in `/usr/local/var/fake_dynamo/db.fdb` and replays it before
starting the server. Because of the way fake_dynamo stores the data,
file size tend to grow by time. so fake_dynamo will compact the database
during start up if the file size is greater than 100mb. you can
manually compact it by passing --compact flag.


[v2]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations.html
[v1]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Appendix.APIv20111205.html
