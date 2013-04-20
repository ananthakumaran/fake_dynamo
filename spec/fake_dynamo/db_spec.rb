require 'spec_helper'

module FakeDynamo
  describe DB do
    let(:data) do
      {
        "TableName" => "Table1",
        "AttributeDefinitions" =>
        [{"AttributeName" => "AttributeName1","AttributeType" => "S"},
         {"AttributeName" => "AttributeName2","AttributeType" => "N"}],
        "KeySchema" =>
        [{"AttributeName" => "AttributeName1","KeyType" => "HASH"},
         {"AttributeName" => "AttributeName2","KeyType" => "RANGE"}],
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    let(:user_table) do
      {"TableName" => "User",
        "AttributeDefinitions" =>
        [{"AttributeName" => "id","AttributeType" => "S"}],
        "KeySchema" =>
        [{"AttributeName" => "id","KeyType" => "HASH"}],
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    let(:user_table_a) do
      {"TableName" => "User",
        "AttributeDefinitions" =>
        [{"AttributeName" => "id","AttributeType" => "S"},
         {"AttributeName" => "age","AttributeType" => "S"},
         {"AttributeName" => "name","AttributeType" => "S"}],
        "KeySchema" =>
        [{"AttributeName" => "id","KeyType" => "HASH"},
         {"AttributeName" => "age", "KeyType" => "RANGE"}],
        "LocalSecondaryIndexes" =>
        [{"IndexName" => "age",
           "KeySchema" =>
           [{"AttributeName" => "id", "KeyType" => "HASH"},
            {"AttributeName" => "name", "KeyType" => "RANGE"}],
           "Projection" => {
             "ProjectionType" => "INCLUDE",
             "NonKeyAttributes" => ["name", "gender"]
           }
         }],
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    context 'CreateTable' do
      it 'should not allow to create duplicate tables' do
        subject.create_table(data)
        expect { subject.create_table(data) }.to raise_error(ResourceInUseException, /duplicate/i)
      end

      it 'should allow to create table with secondary indexes' do
        subject.create_table(user_table_a)
      end

      it 'should fail on extra attribute' do
        user_table_a['AttributeDefinitions'] << {"AttributeName" => "gender","AttributeType" => "S"}
        expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /some attributedefinitions.*not.*used/i)
      end

      it 'should fail on missing attribute' do
        user_table_a['AttributeDefinitions'].delete_at(1)
        expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /some.*attributes.*not.*defined/i)
      end

      context 'LocalSecondaryIndex' do
        let(:lsi) { user_table_a['LocalSecondaryIndexes'][0] }

        it 'should fail if range key is missing' do
          lsi['KeySchema'].delete_at(1)
          expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /not.*range.*key/i)
        end

        it 'should fail on duplicate index names' do
          duplicate = lsi.clone()
          user_table_a['LocalSecondaryIndexes'] << duplicate
          expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /duplicate index/i)
        end

        it 'should fail on different hash key' do
          lsi['KeySchema'][0]['AttributeName'] = 'age'
          expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /not have.*same.*hash key/i)
        end

        context 'projection' do
          let(:projection) { user_table_a['LocalSecondaryIndexes'][0]['Projection'] }

          it 'should fail if non key attributes are specified unnecessarily' do
            projection['ProjectionType'] = 'KEYS_ONLY'
            expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /NonKeyAttributes.*specified/i)
          end

          it 'should fail if non key attributes are not specified' do
            projection.delete('NonKeyAttributes')
            expect { subject.create_table(user_table_a) }.to raise_error(ValidationException, /NonKeyAttributes.*not.*specified/i)
          end
        end
      end
    end

    it 'should fail on unknown operation' do
      expect { subject.process('unknown', data) }.to raise_error(UnknownOperationException, /unknown/i)
    end

    context 'DescribeTable' do
      it 'should describe table' do
        table = subject.create_table(data)
        description = subject.describe_table({'TableName' => 'Table1'})
        description['Table'].should include({
          "ItemCount"=>0,
          "TableSizeBytes"=>0})
      end

      it 'should fail on unavailable table' do
        expect { subject.describe_table({'TableName' => 'Table1'}) }.to raise_error(ResourceNotFoundException, /table1 not found/i)
      end

      it 'should fail on invalid payload' do
        expect { subject.process('DescribeTable', {}) }.to raise_error(ValidationException, /null/)
      end
    end

    context 'DeleteTable' do
      it "should delete table" do
        subject.create_table(data)
        response = subject.delete_table(data)
        subject.tables.should be_empty
        response['TableDescription']['TableStatus'].should == 'DELETING'
      end

      it "should not allow to delete the same table twice" do
        subject.create_table(data)
        subject.delete_table(data)
        expect { subject.delete_table(data) }.to raise_error(ResourceNotFoundException, /table1 not found/i)
      end
    end

    context 'ListTable' do
      before :each do
        (1..5).each do |i|
          data['TableName'] = "Table#{i}"
          subject.create_table(data)
        end
      end

      it "should list all table" do
        result = subject.list_tables({})
        result.should eq({"TableNames"=>["Table1", "Table2", "Table3", "Table4", "Table5"]})
      end

      it 'should handle limit and exclusive_start_table_name' do
        result = subject.list_tables({'Limit' => 3,
                                       'ExclusiveStartTableName' => 'Table1'})
        result.should eq({'TableNames'=>["Table2", "Table3", "Table4"],
                           'LastEvaluatedTableName' => "Table4"})

        result = subject.list_tables({'Limit' => 3,
                                       'ExclusiveStartTableName' => 'Table2'})
        result.should eq({'TableNames' => ['Table3', 'Table4', 'Table5']})

        result = subject.list_tables({'ExclusiveStartTableName' => 'blah'})
        result.should eq({"TableNames"=>["Table1", "Table2", "Table3", "Table4", "Table5"]})
      end

      it 'should validate payload' do
        expect { subject.process('ListTables', {'Limit' => 's'}) }.to raise_error(ValidationException)
      end
    end

    context 'UpdateTable' do

      it 'should update throughput' do
        subject.create_table(data)
        response = subject.update_table({'TableName' => 'Table1',
                               'ProvisionedThroughput' => {
                                 'ReadCapacityUnits' => 7,
                                 'WriteCapacityUnits' => 15
                               }})

        response['TableDescription'].should include({'TableStatus' => 'UPDATING'})
      end

      it 'should handle validation' do
        subject.create_table(data)
        expect { subject.process('UpdateTable', {'TableName' => 'Table1'}) }.to raise_error(ValidationException, /null/)
      end
    end

    context 'delegate to table' do
      subject do
        db = DB.new
        db.create_table(data)
        db
      end

      let(:item) do
        { 'TableName' => 'Table1',
          'Item' => {
            'AttributeName1' => { 'S' => "test" },
            'AttributeName2' => { 'N' => '11' },
            'AttributeName3' => { 'S' => "another" }
          }}
      end

      it 'should delegate to table' do
        subject.process('PutItem', item)
        subject.process('GetItem', {
                          'TableName' => 'Table1',
                          'Key' => {
                            'AttributeName1' => { 'S' => 'test' },
                            'AttributeName2' => { 'N' => '11' }
                          },
                          'AttributesToGet' => ['AttributeName3']
                        })
        subject.process('DeleteItem', {
                          'TableName' => 'Table1',
                          'Key' => {
                            'AttributeName1' => { 'S' => 'test' },
                            'AttributeName2' => { 'N' => '11' }
                          }})
        subject.process('UpdateItem', {
                          'TableName' => 'Table1',
                          'Key' => {
                            'AttributeName1' => { 'S' => 'test' },
                            'AttributeName2' => { 'N' => '11' }
                          },
                          'AttributeUpdates' =>
                          {'AttributeName3' =>
                            {'Value' => {'S' => 'AttributeValue3_New'},
                              'Action' => 'PUT'}
                          },
                          'ReturnValues' => 'ALL_NEW'
                        })

        subject.process('Query', {
                          'TableName' => 'Table1',
                          'Limit' => 5,
                          'Count' => true,
                          'HashKeyValue' => {'S' => 'att1'},
                          'RangeKeyCondition' => {
                            'AttributeValueList' => [{'N' => '1'}],
                            'ComparisonOperator' => 'GT'
                          },
                          'ScanIndexForward' => true
                        })
      end
    end

    context 'batch get item' do
      subject do
        db = DB.new
        db.create_table(data)
        db.create_table(user_table)

        db.put_item({ 'TableName' => 'Table1',
                      'Item' => {
                        'AttributeName1' => { 'S' => "test" },
                        'AttributeName2' => { 'N' => '11' },
                        'AttributeName3' => { 'S' => "another" }
                      }})

        db.put_item({'TableName' => 'User',
                      'Item' => { 'id' => { 'S' => '1' }}
                    })
        db.put_item({'TableName' => 'User',
                      'Item' => { 'id' => { 'S' => '2' }}
                    })
        db
      end

      it 'should validate payload' do
        expect {
          subject.process('BatchGetItem', {})
        }.to raise_error(FakeDynamo::ValidationException)
      end

      it 'should return items' do
        response = subject.process('BatchGetItem', { 'RequestItems' =>
                                     {
                                       'User' => {
                                         'Keys' => [{ 'id' => { 'S' => '1' }},
                                                    { 'id' => { 'S' => '2' }}]
                                       },
                                       'Table1' => {
                                         'Keys' => [{'AttributeName1' => { 'S' => 'test' },
                                                      'AttributeName2' => { 'N' => '11' }}],
                                         'AttributesToGet' => ['AttributeName1', 'AttributeName2']
                                       }
                                     }})

        response.should eq({"Responses"=>
                             {"User"=>
                               {"ConsumedCapacityUnits"=>1,
                                 "Items"=>[{"id"=>{"S"=>"1"}}, {"id"=>{"S"=>"2"}}]},
                               "Table1"=>
                               {"ConsumedCapacityUnits"=>1,
                                 "Items"=>
                                 [{"AttributeName1"=>{"S"=>"test"},
                                    "AttributeName2"=>{"N"=>"11"}}]}},
                             "UnprocessedKeys"=>{}})
      end

      it 'should handle missing items' do
        response = subject.process('BatchGetItem', { 'RequestItems' =>
                                     {
                                       'User' => {
                                         'Keys' => [{ 'id' => { 'S' => '1' }},
                                                    { 'id' => { 'S' => 'asd' }}]
                                       }
                                     }})
        response.should eq({"Responses"=>
                             {"User"=>
                               {"ConsumedCapacityUnits"=>1,
                                 "Items"=>[{"id"=>{"S"=>"1"}}]}},
                             "UnprocessedKeys"=>{}})
      end

      it 'should fail if table not found' do
        expect {
          subject.process('BatchGetItem', { 'RequestItems' =>
                            {
                              'xxx' => {
                                'Keys' => [{ 'AttributeName1' => { 'S' => '1' }},
                                           { 'AttributeName1' => { 'S' => 'asd' }}]}
                            }})
        }.to raise_error(FakeDynamo::ResourceNotFoundException)
      end
    end

    context 'BatchWriteItem' do
      subject do
        db = DB.new
        db.create_table(user_table)
        db
      end

      let(:consumed_capacity) { {'ConsumedCapacity' => { 'CapacityUnits' => 1, 'TableName' => 'User' }} }

      it 'should validate payload' do
        expect {
          subject.process('BatchWriteItem', {})
        }.to raise_error(FakeDynamo::ValidationException)
      end

      it 'should fail if table not found' do
        expect {
          subject.process('BatchWriteItem', {
                            'RequestItems' => {
                              'xxx' => ['DeleteRequest' => { 'Key' => { 'AttributeName1' => { 'S' => 'ananth' }}}]
                            }
                          })
        }.to raise_error(FakeDynamo::ResourceNotFoundException, /table.*not.*found/i)
      end

      it 'should fail on conflict items' do
        expect {
        subject.process('BatchWriteItem', {
                          'RequestItems' => {
                            'User' => [{ 'DeleteRequest' => { 'Key' => { 'id' => { 'S' => 'ananth' }}}},
                                       { 'DeleteRequest' => { 'Key' => { 'id' => { 'S' => 'ananth' }}}}]
                          }
                        })
        }.to raise_error(FakeDynamo::ValidationException, /duplicate/i)

        expect {
          subject.process('BatchWriteItem', {
                            'RequestItems' => {
                              'User' => [{ 'DeleteRequest' => { 'Key' => { 'id' => { 'S' => 'ananth' }}}},
                                         {'PutRequest' => {'Item' => { 'id' => { 'S' => 'ananth'}}}}]
                            }
                          })
        }.to raise_error(FakeDynamo::ValidationException, /duplicate/i)

        expect {
          subject.process('BatchWriteItem', {
                            'RequestItems' => {
                              'User' => [{'PutRequest' => {'Item' => { 'id' => { 'S' => 'ananth'}}}},
                                         {'PutRequest' => {'Item' => { 'id' => { 'S' => 'ananth'}}}}]
                            }
                          })
        }.to raise_error(FakeDynamo::ValidationException, /duplicate/i)
      end

      it 'writes/deletes item in the db' do
        response = subject.process('BatchWriteItem', {
                                     'RequestItems' => {
                                       'User' => [{'PutRequest' => {'Item' => { 'id' => { 'S' => 'ananth'}}}}]
                                     },
                                     'ReturnConsumedCapacity' => 'TOTAL'
                                   })

        response['Responses'].should eq('User' => consumed_capacity )

        response = subject.get_item({'TableName' => 'User',
                                      'Key' => {'id' => { 'S' => 'ananth'}}})

        response['Item']['id'].should eq('S' => 'ananth')

        subject.process('BatchWriteItem', {
                          'RequestItems' => {
                            'User' => [{ 'DeleteRequest' => { 'Key' => { 'id' => { 'S' => 'ananth' }}}}]
                          }
                        })

        response = subject.get_item({'TableName' => 'User',
                                      'Key' => {'id' => { 'S' => 'ananth'}},
                                      'ReturnConsumedCapacity' => 'TOTAL'})

        response.should eq(consumed_capacity)
      end

      it 'fails it the requested operation is more than 25' do
        expect {
          requests = (1..26).map { |i| { 'DeleteRequest' => { 'Key' => { 'id' => { 'S' => "ananth#{i}" }}}} }

          subject.process('BatchWriteItem', {
                            'RequestItems' => {
                              'User' => requests
                            }
                          })

        }.to raise_error(FakeDynamo::ValidationException, /too many items/i)
      end

      it 'should fail on request size greater than 1 mb' do
        expect {

          keys = { 'SS' => (1..2000).map { |i| 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' + i.to_s } }

          requests = (1..25).map do |i|
            {'PutRequest' =>
              {'Item' =>
                { 'id' => { 'S' => 'ananth' + i.to_s },
                  'keys' => keys
                }}}
          end


          subject.process('BatchWriteItem', {
                            'RequestItems' => {
                              'User' => requests
                            }
                          })

        }.to raise_error(FakeDynamo::ValidationException, /size.*exceed/i)
      end
    end
  end
end
