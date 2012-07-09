# FakeDynamo ![Build Status](https://secure.travis-ci.org/ananthakumaran/fake_dynamo.png)

local hosted, inmemory dynamodb emulator.


# Caveats

*  `ConsumedCapacityUnits` value will be 1 always.
*  The response size is not constrained by 1mb limit. So operation
   like `BatchGetItem` will return all items irrespective of the
   response size

# Usage

__requires ruby >= 1.9__

````
gem install fake_dynamo

fake_dynamo --port 4567
````

# Clients

* aws-sdk

````ruby
# rvmsudo fake_dynamo --port 80
AWS.config(:use_ssl => false,
           :dynamo_db_endpoint => 'localhost',
           :access_key_id => "xxx",
           :secret_access_key => "xxx")
````

__please open a pull request with your configuration if you are using
fake_dynamo with clients other than the ones mentioned above__.

# Storage
fake_dynamo stores the `write operations` (request that changes the
data) in `/usr/local/var/fake_dynamo/db.fdb` and replays it before
starting the server. Because of the way fake_dynamo stores the data,
file size tend to grow by time. so fake_dynamo will compact the database
during start up if the file size is greater than 100mb. you can
manually compact it by passing --compact flag.
