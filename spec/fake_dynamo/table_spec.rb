require 'spec_helper'

module FakeDynamo
  describe Table do

    let(:data) do
      {
        "TableName" => "Table1",
        "AttributeDefinitions" =>
        [{"AttributeName" => "name","AttributeType" => "S"},
          {"AttributeName" => "age","AttributeType" => "N"},
          {"AttributeName" => "score","AttributeType" => "N"}],
        "KeySchema" =>
        [{"AttributeName" => "name", "KeyType" => "HASH"},
          {"AttributeName" => "age", "KeyType" => "RANGE"}],
        "LocalSecondaryIndexes" => [{
            "IndexName" => "one",
            "KeySchema" => [{"AttributeName" => "name", "KeyType" => "HASH"},
              {"AttributeName" => "score", "KeyType" => "RANGE"}],
            "Projection" => {
              "ProjectionType" => "ALL"
            }
          }],
        "GlobalSecondaryIndexes" => [{
            "IndexName" => "age_score",
            "KeySchema" => [{"AttributeName" => "age", "KeyType" => "HASH"},
              {"AttributeName" => "score", "KeyType" => "RANGE"}],
            "Projection" => {
              "ProjectionType" => "ALL"
            },
            "ProvisionedThroughput" => {"ReadCapacityUnits" => 5 ,"WriteCapacityUnits" => 10}
          }],
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5 ,"WriteCapacityUnits" => 10}
      }
    end

    let(:item) do
      { 'TableName' => 'Table1',
        'Item' => {
          'name' => { 'S' => "test" },
          'age' => { 'N' => '11' },
          'score' => { 'N' => "14" },
          'binary' => { 'B' => Base64.strict_encode64("binary") },
          'binary_set' => { 'BS' => [Base64.strict_encode64("binary")] }
        },
        'ReturnConsumedCapacity' => 'TOTAL'
      }
    end

    let(:key) do
      {'TableName' => 'Table1',
        'Key' => {
          'name' => { 'S' => 'test' },
          'age' => { 'N' => '11' }
        },
        'ReturnConsumedCapacity' => 'TOTAL'}
    end

    let(:consumed_capacity) { {'ConsumedCapacity' => { 'CapacityUnits' => 1, 'TableName' => 'Table1' }} }

    subject { Table.new(data) }

    its(:status) { should == 'CREATING' }
    its(:creation_date_time) { should_not be_nil }

    context '#update' do
      subject do
        table = Table.new(data)
        table.update({ "ProvisionedThroughput" => {
              "ReadCapacityUnits" => 10,
              "WriteCapacityUnits" => 15
            }})
        table
      end

      its(:read_capacity_units) { should == 10 }
      its(:write_capacity_units) { should == 15 }
      its(:last_increased_time) { should be_a_kind_of(Integer) }
      its(:last_decreased_time) { should be_nil }
    end

    let(:gsi_update) do
      { "TableName" => "Table1",
        "GlobalSecondaryIndexUpdates" =>
        [{"Update" => {
              "IndexName" => "age_score",
              "ProvisionedThroughput" => {
                "ReadCapacityUnits" => 100,
                "WriteCapacityUnits" => 15 }
            }
          }
        ]
      }
    end

    context '#update with gsi' do
      subject do
        table = Table.new(data)
        table.update(gsi_update)
        table
      end

      it 'should update global index' do
        subject.find_global_secondary_index('age_score').read_capacity_units.should eq(100)
      end
    end

    context '#put_item' do
      it 'should fail if hash key is not present' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                             'age' => { 'S' => "test" }
                             }})
        end.to raise_error(ValidationException, /missing.*item/i)
      end

      it 'should fail if sets contains duplicates' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => "3" },
                               'score' => { 'NS' => ["1", "3", "3"] }
                             }})
        end.to raise_error(ValidationException, /duplicate/)
      end

      it 'should fail if value is of different type' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => "3" },
                               'score' => { 'NS' => ["1", "3", "one"] }
                             }})
        end.to raise_error(ValidationException, /numeric/)

        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => "3" },
                               'score' => { 'N' => "one" }
                             }})
        end.to raise_error(ValidationException, /numeric/)
      end

      it 'should handle float values' do
        subject.put_item({ 'TableName' => 'Table1',
                           'Item' => {
                             'name' => { 'S' => "test" },
                             'age' => { 'N' => "3" },
                             'score' => { 'N' => "4.44444" }
                           }})

        ['3', '3.0', '3e0', '.3e1', '003'].each do |n|
          response = subject.get_item({'TableName' => 'Table1',
                                        'Key' => {
                                          'name' => { 'S' => 'test' },
                                          'age' => { 'N' => n }
                                        }})

          response['Item']['score'].should eq('N' => '4.44444')
        end
      end

      it 'should fail if range key is not present' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" }
                             }})
        end.to raise_error(ValidationException, /missing.*item/i)
      end

      it 'should fail on type mismatch' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'N' => "test" },
                               'age' => { 'N' => '11' }
                             }})
        end.to raise_error(ValidationException, /mismatch/i)
      end

      it 'should fail on index type mismatch' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
              'Item' => {
                'name' => { 'S' => "test" },
                'age' => { 'N' => '11' },
                'score' => { 'S' => 'another' },
              }})
        end.to raise_error(ValidationException, /mismatch/i)
      end

      it 'should fail if the attribute value contains empty string' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => '11' },
                               'x' => { 'S' => '' }
                             }})
        end.to raise_error(ValidationException, /empty/i)

        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => '11' },
                               'x' => { 'SS' => ['x', ''] }
                             }})
        end.to raise_error(ValidationException, /empty/i)
      end

      it 'should fail on empty key value' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => '11' },
                               '' => { 'SS' => ['x'] }
                             }})
        end.to raise_error(ValidationException, /empty/i)
      end

      it 'should fail on empty set' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'name' => { 'S' => "test" },
                               'age' => { 'N' => '11' },
                               'x' => { 'SS' => [] }
                             }})
        end.to raise_error(ValidationException, /empty/i)
      end

      it 'should putitem in the table' do
        subject.put_item(item)
        subject.items.size.should == 1
      end

      context 'Expected & ReturnValues' do
        subject do
          table = Table.new(data)
          table.put_item(item)
          table
        end

        it 'should check condition' do
          [[{}, /set to null/],
           [{'Exists' => true}, /set to true/],
           [{'Exists' => false}],
           [{'Value' => { 'N' => '15' } }],
           [{'Value' => { 'N' => '15' }, 'Exists' => true}],
           [{'Value' => { 'N' => '15' }, 'Exists' => false}, /cannot expect/i]].each do |value, message|

            op = lambda {
              subject.put_item(item.merge({'Expected' => { 'score' => value }}))
            }

            if message
              expect(&op).to raise_error(ValidationException, message)
            else
              expect(&op).to raise_error(ConditionalCheckFailedException)
            end
          end
        end

        it 'should give default response' do
          item['Item']['score'] = { 'N' => "17" }
          subject.put_item(item).should include(consumed_capacity)
        end

        it 'should send old item' do
          old_item = Utils.deep_copy(item)
          new_item = Utils.deep_copy(item)
          new_item['Item']['score'] = { 'N' => "17" }
          new_item.merge!({'ReturnValues' => 'ALL_OLD'})
          subject.put_item(new_item)['Attributes'].should == old_item['Item']
        end
      end
    end

    context '#get_item' do
      subject do
        table = Table.new(data)
        table.put_item(item)
        table
      end

      it 'should return empty when the key is not found' do
        response = subject.get_item({'TableName' => 'Table1',
                                      'Key' => {
                                        'name' => { 'S' => 'xxx' },
                                        'age' => { 'N' => '11' }
                                      }
                                    })
        response.should eq({})
      end

      it 'should filter attributes' do
        response = subject.get_item({'TableName' => 'Table1',
                                      'Key' => {
                                        'name' => { 'S' => 'test' },
                                        'age' => { 'N' => '11' }
                                      },
                                      'AttributesToGet' => ['score', 'xxx'],
                                      'ReturnConsumedCapacity' => 'TOTAL'
                                    })
        response.should eq({ 'Item' => { 'score' => { 'N' => '14'}}}
            .merge(consumed_capacity))
      end
    end

    context '#delete_item' do
      subject do
        table = Table.new(data)
        table.put_item(item)
        table
      end

      it 'should delete item' do
        response = subject.delete_item(key)
        response.should eq(consumed_capacity)
      end

      it 'should be idempotent' do
        response_1 = subject.delete_item(key)
        response_2 = subject.delete_item(key)

        response_1.should == response_2
      end

      it 'should check conditions' do
        expect do
          subject.delete_item(key.merge({'Expected' =>
                                          {'score' => { 'Exists' => false }}}))
        end.to raise_error(ConditionalCheckFailedException)

        response = subject.delete_item(key.merge({'Expected' =>
                                                   {'score' =>
                                                     {'Value' => { 'N' => '14'}}}}))
        response.should eq(consumed_capacity)

        expect do
          subject.delete_item(key.merge({'Expected' =>
                                          {'score' =>
                                            {'Value' => { 'N' => '14'}}}}))
        end.to raise_error(ConditionalCheckFailedException)
      end

      it 'should return old value' do
        response = subject.delete_item(key.merge('ReturnValues' => 'ALL_OLD'))
        response.should eq(consumed_capacity.merge({'Attributes' => item['Item']}))
      end
    end

    context '#update_item' do
      subject do
        table = Table.new(data)
        table.put_item(item)
        table
      end

      let(:put) do
        {'AttributeUpdates' => {'score' => { 'Value' => { 'N' => '18' },
            'Action' => 'PUT'}}}
      end

      let(:delete) do
        {'AttributeUpdates' => {'score' => {'Action' => 'DELETE'}}}
      end

      it "should not partially update item" do
        expect do
          put['AttributeUpdates'].merge!({ 'xx' => { 'Value' => { 'N' => 'one'}, 'Action' => 'ADD'}})
          subject.update_item(key.merge(put))
        end.to raise_error(ValidationException, /numeric/)
        subject.get_item(key).should include('Item' => item['Item'])

        expect do
          key['Key']['name']['S'] = 'unknown'
          put['AttributeUpdates'].merge!({ 'xx' => { 'Value' => { 'N' => 'one'}, 'Action' => 'ADD'}})
          subject.update_item(key.merge(put))
        end.to raise_error(ValidationException, /numeric/)

        subject.get_item(key).should eq(consumed_capacity)
      end

      it "should check conditions" do
        expect do
          subject.update_item(key.merge({'Expected' =>
                                          {'score' => { 'Exists' => false }}}))
        end.to raise_error(ConditionalCheckFailedException)
      end

      it "should check index types" do
        expect do
          put['AttributeUpdates']['score']['Value'] = {'S' => 'another'}
          subject.update_item(key.merge(put))
        end.to raise_error(ValidationException, /mismatch/i)
      end

      it "should create new item if the key doesn't exist" do
        key['Key']['name']['S'] = 'new'
        subject.update_item(key.merge(put))
        subject.get_item(key).should include( "Item"=>
                                              {"name"=>{"S"=>"new"},
                                                "age"=>{"N"=>"11"},
                                                "score"=>{"N"=>"18"}})
      end

      it "shouldn't create a new item if key doesn't exist and action is delete" do
        key['Key']['name']['S'] = 'new'
        subject.update_item(key.merge(delete))
        subject.get_item(key).should eq(consumed_capacity)
      end

      it "should handle return values" do
        data = key.merge(put).merge({'ReturnValues' => 'UPDATED_NEW'})
        subject.update_item(data).should include({'Attributes' => { 'score' => { 'N' => '18'}}})
      end
    end

    context '#return_values' do
      let(:put) do
        {'AttributeUpdates' => {'score' => { 'Value' => { 'N' => '19' },
              'Action' => 'PUT'}}}
      end

      it "should return values" do
        [['ALL_OLD', {'x' => 'y'}, nil, {"Attributes" => {'x' => 'y'}}],
         ['ALL_NEW', nil, {'x' => 'y'}, {"Attributes" => {'x' => 'y'}}],
         ['NONE', nil, nil, {}]].each do |return_value, old_item, new_item, response|
          data = {'ReturnValues' => return_value }
          subject.return_values(data, old_item, new_item).should eq(response)
        end
        expect { subject.return_values({'ReturnValues' => 'asdf'}, nil, nil) }.to raise_error(/unknown/)
      end

      it "should return update old value" do
        subject.put_item(item)
        data = key.merge(put).merge({'ReturnValues' => 'UPDATED_OLD'})
        subject.update_item(data).should include({'Attributes' => { 'score' => { 'N' => '14'}}})
      end

      it "should return update new value" do
        subject.put_item(item)
        data = key.merge(put).merge({'ReturnValues' => 'UPDATED_NEW'})
        subject.update_item(data).should include({'Attributes' => { 'score' => { 'N' => '19'}}})
      end
    end

    context '#query' do
      subject do
        t = Table.new(data)
        (1..3).each do |i|
          (15.downto(1)).each do |j|
            next if j.even?
            item['Item']['name']['S'] = "att#{i}"
            item['Item']['age']['N'] = j.to_s
            item['Item']['score'] = {'N' => ((j % 3) + 2).to_s}
            t.put_item(item)
          end
        end
        t
      end

      let(:query) do
        {
          'TableName' => 'Table1',
          'Limit' => 5,
          'KeyConditions' => {
            'name' => {
              'AttributeValueList' => [{'S' => 'att1'}],
              'ComparisonOperator' => 'EQ'
            },
            'age' => {
              'AttributeValueList' => [{'N' => '1'}],
              'ComparisonOperator' => 'GT'
            }
          },
          'ScanIndexForward' => true
        }
      end

      let(:lsi_query) do
        {
          'TableName' => 'Table1',
          'Limit' => 5,
          'IndexName' => 'one',
          'KeyConditions' => {
            'name' => {
              'AttributeValueList' => [{'S' => 'att1'}],
              'ComparisonOperator' => 'EQ'
            },
            'score' => {
              'AttributeValueList' => [{'N' => '1'}],
              'ComparisonOperator' => 'GT'
            }
          },
          'ScanIndexForward' => true
        }
      end

      let(:gsi_query) do
        {
          'TableName' => 'Table1',
          'IndexName' => 'age_score',
          'KeyConditions' => {
            'age' => {
              'AttributeValueList' => [{'N' => '8'}],
              'ComparisonOperator' => 'EQ'
            }
          },
          'ScanIndexForward' => true
        }
      end

      let(:query_with_filter) do
        {
          'TableName' => 'Table1',
          'Limit' => 5,
          'KeyConditions' => {
            'name' => {
              'AttributeValueList' => [{'S' => 'att1'}],
              'ComparisonOperator' => 'EQ'
            },
          },
          'QueryFilter' => {
            'score' => {
              'AttributeValueList' => [{'N' => '3'}],
              'ComparisonOperator' => 'GT'
            }
          },
          'ScanIndexForward' => true
        }
      end

      context 'query projection' do
        let(:query) do
          {
            'TableName' => 'Table1',
            'Limit' => 5,
            'Select' => 'ALL_PROJECTED_ATTRIBUTES',
            'IndexName' => 'one',
            'KeyConditions' => {
              'name' => {
                'AttributeValueList' => [{'S' => 'test'}],
                'ComparisonOperator' => 'EQ'
              }
            },
            'ScanIndexForward' => true
          }
        end

        let(:projection) { data['LocalSecondaryIndexes'][0]['Projection'] }

        it 'should return all attributes' do
          t = Table.new(data)
          t.put_item(item)
          response = t.query(query)
          response['Items'].first.keys.size.should eq(5)
        end

        it 'should return return only the keys' do
          projection['ProjectionType'] = 'KEYS_ONLY'
          t = Table.new(data)
          t.put_item(item)
          response = t.query(query)
          response['Items'].first.keys.size.should eq(3)
        end

        it 'should return return only the non key attributes' do
          projection['ProjectionType'] = 'INCLUDE'
          projection['NonKeyAttributes'] = ['binary_set']
          t = Table.new(data)
          t.put_item(item)
          response = t.query(query)
          response['Items'].first.keys.size.should eq(4)
        end

        it 'should return last evaluated key when the item processed size exceeds 1 mb' do
          t = Table.new(data)
          t.put_item(item)
          25.times do |i|
            t.put_item({'TableName' => 'User',
                'Item' => { 'name' => { 'S' => '1' },
                  'age' => { 'N' => i},
                  'payload' => { 'S' => ('x' * 50 * 1024) }}})
          end
          query['KeyConditions']['name']['AttributeValueList'] = [{'S' => '1'}]
          query['KeyConditions'].delete('age')
          response = t.query(query)
          response['LastEvaluatedKey'].should_not be_empty
        end
      end

      it 'should not allow count and attributes_to_get simutaneously' do
        expect {
          subject.query({'Select' => 'COUNT', 'AttributesToGet' => ['xx']})
        }.to raise_error(ValidationException, /count/i)
      end

      it 'should not allow to query on a table without rangekey' do
        data['KeySchema'].delete_at(1)
        data['AttributeDefinitions'].delete_at(1)
        data['AttributeDefinitions'].delete_at(1)
        data.delete('LocalSecondaryIndexes')
        data.delete('GlobalSecondaryIndexes')
        t = Table.new(data)
        expect {
          t.query(query)
        }.to raise_error(ValidationException, /key schema/)
      end

      it 'should only allow limit greater than zero' do
        expect {
          subject.query(query.merge('Limit' => 0))
        }.to raise_error(ValidationException, /limit/i)
      end

      it 'should handle basic query' do
        result = subject.query(query)
        result['Count'].should eq(5)
      end

      it 'should handle scanindexforward' do
        result = subject.query(query)
        result['Items'].first['age'].should eq({'N' => '3'})
        result = subject.query(query.merge({'ScanIndexForward' => false}))
        result['Items'].first['age'].should eq({'N' => '15'})

        query['ExclusiveStartKey'] = { 'name' => { 'S' => 'att1' }, 'age' => { "N" => '7' }}
        result = subject.query(query)
        result['Items'][0]['name'].should eq({'S' => 'att1'})
        result['Items'][0]['age'].should eq({'N' => '9'})

        result = subject.query(query.merge({'ScanIndexForward' => false}))
        result['Items'][0]['name'].should eq({'S' => 'att1'})
        result['Items'][0]['age'].should eq({'N' => '5'})

        query['ExclusiveStartKey'] = { 'name' => { 'S' => 'att1' }, 'age' => { "N" => '8' }}
        result = subject.query(query)
        result['Items'][0]['name'].should eq({'S' => 'att1'})
        result['Items'][0]['age'].should eq({'N' => '9'})

        result = subject.query(query.merge({'ScanIndexForward' => false}))
        result['Items'][0]['name'].should eq({'S' => 'att1'})
        result['Items'][0]['age'].should eq({'N' => '7'})
      end

      it 'should return lastevaluated key' do
        result = subject.query(query)
        result['LastEvaluatedKey'].should == {"name"=>{"S"=>"att1"}, "age"=>{"N"=>"11"}}
        result = subject.query(query.merge('Limit' => 100))
        result['LastEvaluatedKey'].should be_nil

        query.delete('Limit')
        result = subject.query(query)
        result['LastEvaluatedKey'].should be_nil
      end

      it 'should handle exclusive start key' do
        result = subject.query(query.merge({'ExclusiveStartKey' => {"name"=>{"S"=>"att1"}, "age"=>{"N"=>"7"}}}))
        result['Count'].should eq(4)
        result['Items'].first['age'].should eq({'N' => '9'})
        result = subject.query(query.merge({'ExclusiveStartKey' => {"name"=>{"S"=>"att1"}, "age"=>{"N"=>"8"}}}))
        result['Count'].should eq(4)
        result['Items'].first['age'].should eq({'N' => '9'})
        result = subject.query(query.merge({'ExclusiveStartKey' => {"name"=>{"S"=>"att1"}, "age"=>{"N"=>"88"}}}))
        result['Count'].should eq(0)
        result['Items'].should be_empty
      end

      it 'should return all elements if rangekeycondition is not given' do
        query['KeyConditions'].delete('age')
        result = subject.query(query)
        result['Count'].should eq(5)
      end

      it 'should handle between operator' do
        query['KeyConditions']['age'] = {
          'AttributeValueList' => [{'N' => '1'}, {'N' => '7'}],
            'ComparisonOperator' => 'BETWEEN'
        }
        result = subject.query(query)
        result['Count'].should eq(4)
      end

      it 'should handle attributes_to_get' do
        query['AttributesToGet'] = ['name', "age"]
        result = subject.query(query)
        result['Items'].first.should eq('name' => { 'S' => 'att1'},
                                        'age' => { 'N' => '3' })
      end

      context "local secondary query" do
        it 'should fail if index name is missing' do
          lsi_query.delete('IndexName')
          expect { subject.query(lsi_query) }.to raise_error(ValidationException, /missed.*key/i)
        end

        it 'should fail on invalid index name' do
          lsi_query['IndexName'] = 'xxx'
          expect { subject.query(lsi_query) }.to raise_error(ValidationException, /does not have the specified index/)
        end

        it 'should fail if hash condition is missing' do
          lsi_query['KeyConditions'].delete('name')
          expect { subject.query(lsi_query) }.to raise_error(ValidationException, /missed.*key.*schema/i)
        end

        it 'should fail if hash condition is not EQ' do
          lsi_query['KeyConditions']['name']['ComparisonOperator'] = 'GT'
          expect { subject.query(lsi_query) }.to raise_error(ValidationException, /condition not supported/i)
        end

        it 'should handle index query' do
          result = subject.query(lsi_query)
          result['Count'].should eq(5)
        end

        it 'should sort based on lsi range key' do
          lsi_query.delete('Limit')
          result = subject.query(lsi_query)
          keys = result['Items'].map { |i| [i['score']['N'].to_i, i['age']['N'].to_i] }
          keys.should eq(keys.sort)
        end

        it 'should handle attributes_to_get within index' do
          lsi_query['AttributesToGet'] = ['name']
          result = subject.query(lsi_query)
          result['Items'].first.should eq('name' => { 'S' => 'att1'})
        end
      end

      context 'global secondary index' do
        subject do
          t = Table.new(data)
          (1..10).each do |i|
            (1..10).each do |j|
              item['Item']['name']['S'] = "att#{i}"
              item['Item']['age'] = { 'N' => j.to_s }
              if j.even?
                item['Item']['score'] = {'N' => (i*10 + j).to_s }
              else
                item['Item'].delete('score')
              end
              t.put_item(item)
            end
          end
          t
        end
        it 'should not support consitent read' do
          gsi_query['ConsistentRead'] = true
          expect { subject.query(gsi_query) }.to raise_error(ValidationException, /consistent/i)
        end

        it 'should not return results if range key is missing' do
          gsi_query['KeyConditions']['age']['AttributeValueList'] = [{'N' => '9'}]
          result = subject.query(gsi_query)
          result['Items'].size.should eq(0)
        end

        it 'should handle global index query with hash' do
          gsi_query['KeyConditions']['age']['AttributeValueList'] = [{'N' => '2'}]
          result = subject.query(gsi_query)
          result['Items'].size.should eq(10)
        end

        it 'should handle global index query with hash & range' do
          gsi_query['KeyConditions'] = {
            'age' => {
              'AttributeValueList' => [{'N' => '2'}],
              'ComparisonOperator' => 'EQ'
            },
            'score' => {
              'AttributeValueList' => [{'N' => '53'}],
              'ComparisonOperator' => 'LT'
            }
          }
          result = subject.query(gsi_query)
          result['Items'].size.should eq(5)
        end

        it 'should sort based on gsi range key' do
          result = subject.query(gsi_query)
          keys = result['Items'].map { |i| [i['score']['N'].to_i] }
          keys.should eq(keys.sort)
        end
      end

      context 'query filter' do
        it 'should require attributes' do
          query_with_filter['QueryFilter']['score'].delete('AttributeValueList')
          expect { subject.query(query_with_filter) }.to raise_error(ValidationException, /invalid.*ComparisonOperator/i)
        end

        it 'should require operator' do
          query_with_filter['QueryFilter']['score'].delete('ComparisonOperator')
          expect { subject.query(query_with_filter) }.to raise_error(ValidationException, /missing.*ComparisonOperator/i)
        end

        it 'should handle query filter' do
          result = subject.query(query_with_filter)
          result['Count'].should eq(2)
        end
      end
    end

    context '#scan' do
      subject do
        t = Table.new(data)
        (1..3).each do |i|
          (15.downto(1)).each do |j|
            next if j.even?
            item['Item']['name']['S'] = "att#{i}"
            item['Item']['age']['N'] = j.to_s
            t.put_item(item)
          end
        end
        t
      end

      let(:scan) do
        {
          'TableName' => 'Table1',
          'ScanFilter' => {
            'age' => {
              'AttributeValueList' => [{'N' => '1'}],
              'ComparisonOperator' => 'GE'
            }
          }
        }
      end

      it 'should not allow count and attributes_to_get simutaneously' do
        expect {
          subject.scan({'Select' => 'COUNT', 'AttributesToGet' => ['xx']})
        }.to raise_error(ValidationException, /count/i)
      end

      it 'should only return count' do
        scan['Select'] = 'COUNT'
        response = subject.scan(scan)
        response['Count'].should eq(24)
        response['Items'].should be_nil
      end

      it 'should not allow ALL_PROJECTED_ATTRIBUTES' do
        scan['Select'] = 'ALL_PROJECTED_ATTRIBUTES'
        expect { subject.scan(scan) }.to raise_error(ValidationException, /querying.*indexname/i)
      end

      it 'should only allow limit greater than zero' do
        expect {
          subject.scan(scan.merge('Limit' => 0))
        }.to raise_error(ValidationException, /limit/i)
      end

      it 'should handle basic scan' do
        result = subject.scan(scan)
        result['Count'].should eq(24)

        scan['ScanFilter']['age']['ComparisonOperator'] = 'EQ'
        subject.scan(scan)['Count'].should eq(3)
      end

      it 'should return lastevaluated key' do
        scan['Limit'] = 5
        result = subject.scan(scan)
        result['LastEvaluatedKey'].should == {"name"=>{"S"=>"att1"}, "age"=>{"N"=>"9"}}
        result = subject.scan(scan.merge('Limit' => 100))
        result['LastEvaluatedKey'].should be_nil

        scan.delete('Limit')
        result = subject.scan(scan)
        result['LastEvaluatedKey'].should be_nil
      end

      it 'should handle ordering' do
        scan['ExclusiveStartKey'] = { 'name' => { 'S' => 'att2' }, 'age' => { "N" => '7' }}
        result = subject.scan(scan)
        result['Items'][0]['name'].should eq({'S' => 'att2'})
        result['Items'][0]['age'].should eq({'N' => '9'})

        scan['ExclusiveStartKey'] = { 'name' => { 'S' => 'att2' }, 'age' => { "N" => '8' }}
        result['Items'][0]['name'].should eq({'S' => 'att2'})
        result['Items'][0]['age'].should eq({'N' => '9'})
      end

    end
  end
end
