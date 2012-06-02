require 'spec_helper'

module FakeDynamo
  describe Table do

    let(:data) do
      {
        "TableName" => "Table1",
        "KeySchema" =>
        {"HashKeyElement" => {"AttributeName" => "AttributeName1","AttributeType" => "S"},
          "RangeKeyElement" => {"AttributeName" => "AttributeName2","AttributeType" => "N"}},
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    let(:item) do
      { 'TableName' => 'Table1',
        'Item' => {
          'AttributeName1' => { 'S' => "test" },
          'AttributeName2' => { 'N' => '11' },
          'AttributeName3' => { 'S' => "another" }
        }}
    end

    let(:key) do
      {'TableName' => 'Table1',
        'Key' => {
          'HashKeyElement' => { 'S' => 'test' },
          'RangeKeyElement' => { 'N' => '11' }
        }}
    end

    let(:consumed_capacity) { { 'ConsumedCapacityUnits' => 1 } }

    subject { Table.new(data) }

    its(:status) { should == 'CREATING' }
    its(:creation_date_time) { should_not be_nil }

    context '#update' do
      subject do
        table = Table.new(data)
        table.update(10, 15)
        table
      end

      its(:read_capacity_units) { should == 10 }
      its(:write_capacity_units) { should == 15 }
      its(:last_increased_time) { should be_a_kind_of(Integer) }
      its(:last_decreased_time) { should be_nil }
    end

    context '#put_item' do
      it 'should fail if hash key is not present' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                             'AttributeName2' => { 'S' => "test" }
                             }})
        end.to raise_error(ValidationException, /missing.*item/i)
      end

      it 'should fail if sets contains duplicates' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => "3" },
                               'AttributeName3' => { 'NS' => ["1", "3", "3"] }
                             }})
        end.to raise_error(ValidationException, /duplicate/)
      end

      it 'should fail if value is of different type' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => "3" },
                               'AttributeName3' => { 'NS' => ["1", "3", "one"] }
                             }})
        end.to raise_error(ValidationException, /numeric/)

        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => "3" },
                               'AttributeName3' => { 'N' => "one" }
                             }})
        end.to raise_error(ValidationException, /numeric/)
      end

      it 'should handle float values' do
        subject.put_item({ 'TableName' => 'Table1',
                           'Item' => {
                             'AttributeName1' => { 'S' => "test" },
                             'AttributeName2' => { 'N' => "3" },
                             'AttributeName3' => { 'N' => "4.44444" }
                           }})
        response = subject.get_item({'TableName' => 'Table1',
                                      'Key' => {
                                        'HashKeyElement' => { 'S' => 'test' },
                                        'RangeKeyElement' => { 'N' => '3' }
                                      }})

        response['Item']['AttributeName3'].should eq('N' => '4.44444')


      end

      it 'should fail if range key is not present' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" }
                             }})
        end.to raise_error(ValidationException, /missing.*item/i)
      end

      it 'should fail on type mismatch' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'N' => "test" },
                               'AttributeName2' => { 'N' => '11' }
                             }})
        end.to raise_error(ValidationException, /mismatch/i)
      end

      it 'should fail if the attribute value contains empty string' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => '11' },
                               'x' => { 'S' => '' }
                             }})
        end.to raise_error(ValidationException, /empty/i)

        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => '11' },
                               'x' => { 'SS' => ['x', ''] }
                             }})
        end.to raise_error(ValidationException, /empty/i)
      end

      it 'should fail on empty key value' do
        expect do
          subject.put_item({ 'TableName' => 'Table1',
                             'Item' => {
                               'AttributeName1' => { 'S' => "test" },
                               'AttributeName2' => { 'N' => '11' },
                               '' => { 'SS' => ['x'] }
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
           [{'Value' => { 'S' => 'xxx' } }],
           [{'Value' => { 'S' => 'xxx' }, 'Exists' => true}],
           [{'Value' => { 'S' => 'xxx' }, 'Exists' => false}, /cannot expect/i]].each do |value, message|

            op = lambda {
              subject.put_item(item.merge({'Expected' => { 'AttributeName3' => value }}))
            }

            if message
              expect(&op).to raise_error(ValidationException, message)
            else
              expect(&op).to raise_error(ConditionalCheckFailedException)
            end
          end
        end

        it 'should give default response' do
          item['Item']['AttributeName3'] = { 'S' => "new" }
          subject.put_item(item).should include(consumed_capacity)
        end

        it 'should send old item' do
          old_item = Utils.deep_copy(item)
          new_item = Utils.deep_copy(item)
          new_item['Item']['AttributeName3'] = { 'S' => "new" }
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
                                        'HashKeyElement' => { 'S' => 'xxx' },
                                        'RangeKeyElement' => { 'N' => '11' }
                                      }
                                    })
        response.should eq(consumed_capacity)
      end

      it 'should filter attributes' do
        response = subject.get_item({'TableName' => 'Table1',
                                      'Key' => {
                                        'HashKeyElement' => { 'S' => 'test' },
                                        'RangeKeyElement' => { 'N' => '11' }
                                      },
                                      'AttributesToGet' => ['AttributeName3', 'xxx']
                                    })
        response.should eq({ 'Item' => { 'AttributeName3' => { 'S' => 'another'}},
                             'ConsumedCapacityUnits' => 1})
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
                                          {'AttributeName3' => { 'Exists' => false }}}))
        end.to raise_error(ConditionalCheckFailedException)

        response = subject.delete_item(key.merge({'Expected' =>
                                                   {'AttributeName3' =>
                                                     {'Value' => { 'S' => 'another'}}}}))
        response.should eq(consumed_capacity)

        expect do
          subject.delete_item(key.merge({'Expected' =>
                                          {'AttributeName3' =>
                                            {'Value' => { 'S' => 'another'}}}}))
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
        {'AttributeUpdates' => {'AttributeName3' => { 'Value' => { 'S' => 'updated' },
            'Action' => 'PUT'}}}
      end

      let(:delete) do
        {'AttributeUpdates' => {'AttributeName3' => {'Action' => 'DELETE'}}}
      end

      it "should not partially update item" do
        expect do
          put['AttributeUpdates'].merge!({ 'xx' => { 'Value' => { 'N' => 'one'}, 'Action' => 'ADD'}})
          subject.update_item(key.merge(put))
        end.to raise_error(ValidationException, /numeric/)
        subject.get_item(key).should include('Item' => item['Item'])

        expect do
          key['Key']['HashKeyElement']['S'] = 'unknown'
          put['AttributeUpdates'].merge!({ 'xx' => { 'Value' => { 'N' => 'one'}, 'Action' => 'ADD'}})
          subject.update_item(key.merge(put))
        end.to raise_error(ValidationException, /numeric/)

        subject.get_item(key).should eq(consumed_capacity)
      end

      it "should check conditions" do
        expect do
          subject.update_item(key.merge({'Expected' =>
                                          {'AttributeName3' => { 'Exists' => false }}}))
        end.to raise_error(ConditionalCheckFailedException)
      end

      it "should create new item if the key doesn't exist" do
        key['Key']['HashKeyElement']['S'] = 'new'
        subject.update_item(key.merge(put))
        subject.get_item(key).should include( "Item"=>
                                              {"AttributeName1"=>{"S"=>"new"},
                                                "AttributeName2"=>{"N"=>"11"},
                                                "AttributeName3"=>{"S"=>"updated"}})
      end

      it "shouldn't create a new item if key doesn't exist and action is delete" do
        key['Key']['HashKeyElement']['S'] = 'new'
        subject.update_item(key.merge(delete))
        subject.get_item(key).should eq(consumed_capacity)
      end

      it "should handle return values" do
        data = key.merge(put).merge({'ReturnValues' => 'UPDATED_NEW'})
        subject.update_item(data).should include({'Attributes' => { 'AttributeName3' => { 'S' => 'updated'}}})
      end
    end

    context '#return_values' do
      let(:put) do
        {'AttributeUpdates' => {'AttributeName3' => { 'Value' => { 'S' => 'updated' },
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
        subject.update_item(data).should include({'Attributes' => { 'AttributeName3' => { 'S' => 'another'}}})
      end

      it "should return update new value" do
        subject.put_item(item)
        data = key.merge(put).merge({'ReturnValues' => 'UPDATED_NEW'})
        subject.update_item(data).should include({'Attributes' => { 'AttributeName3' => { 'S' => 'updated'}}})
      end
    end

    context '#query' do
      subject do
        t = Table.new(data)
        t.put_item(item)
        (1..3).each do |i|
          (1..15).each do |j|
            next if j.even?
            item['Item']['AttributeName1']['S'] = "att#{i}"
            item['Item']['AttributeName2']['N'] = j.to_s
            t.put_item(item)
          end
        end
        t
      end

      let(:query) do
        {
          'TableName' => 'Table1',
          'Limit' => 5,
          'HashKeyValue' => {'S' => 'att1'},
          'RangeKeyCondition' => {
            'AttributeValueList' => [{'N' => '1'}],
            'ComparisonOperator' => 'GT'
          },
          'ScanIndexForward' => true
        }
      end

      it 'should not allow count and attributes_to_get simutaneously' do
        expect {
          subject.query({'Count' => 0, 'AttributesToGet' => ['xx']})
        }.to raise_error(ValidationException, /count/i)
      end

      it 'should not allow to query on a table without rangekey' do
        data['KeySchema'].delete('RangeKeyElement')
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
        result['Items'].first['AttributeName2'].should eq({'N' => '3'})
        result = subject.query(query.merge({'ScanIndexForward' => false}))
        result['Items'].first['AttributeName2'].should eq({'N' => '15'})
      end

      it 'should return lastevaluated key' do
        result = subject.query(query)
        result['LastEvaluatedKey'].should == {"HashKeyElement"=>{"S"=>"att1"}, "RangeKeyElement"=>{"N"=>"11"}}
        result = subject.query(query.merge('Limit' => 100))
        result['LastEvaluatedKey'].should be_nil

        query.delete('Limit')
        result = subject.query(query)
        result['LastEvaluatedKey'].should be_nil
      end

      it 'should handle exclusive start key' do
        result = subject.query(query.merge({'ExclusiveStartKey' => {"HashKeyElement"=>{"S"=>"att1"}, "RangeKeyElement"=>{"N"=>"7"}}}))
        result['Count'].should eq(4)
        result['Items'].first['AttributeName2'].should eq({'N' => '9'})        
        result = subject.query(query.merge({'ExclusiveStartKey' => {"HashKeyElement"=>{"S"=>"att1"}, "RangeKeyElement"=>{"N"=>"8"}}}))
        result['Count'].should eq(4)
        result['Items'].first['AttributeName2'].should eq({'N' => '9'})
        result = subject.query(query.merge({'ExclusiveStartKey' => {"HashKeyElement"=>{"S"=>"att1"}, "RangeKeyElement"=>{"N"=>"88"}}}))
        result['Count'].should eq(0)
        result['Items'].should be_empty
      end

      it 'should return all elements if not rangekeycondition is given' do
        query.delete('RangeKeyCondition')
        result = subject.query(query)
        result['Count'].should eq(5)
      end

      it 'should handle between operator' do
        query['RangeKeyCondition'] = {
          'AttributeValueList' => [{'N' => '1'}, {'N' => '7'}],
            'ComparisonOperator' => 'BETWEEN'
        }
        result = subject.query(query)
        result['Count'].should eq(4)
      end

      it 'should handle attributes_to_get' do
        query['AttributesToGet'] = ['AttributeName1', "AttributeName2"]
        result = subject.query(query)
        result['Items'].first.should eq('AttributeName1' => { 'S' => 'att1'},
                                        'AttributeName2' => { 'N' => '3' })
      end
    end

    context '#scan' do
      subject do
        t = Table.new(data)
        (1..3).each do |i|
          (1..15).each do |j|
            next if j.even?
            item['Item']['AttributeName1']['S'] = "att#{i}"
            item['Item']['AttributeName2']['N'] = j.to_s
            t.put_item(item)
          end
        end
        t
      end

      let(:scan) do
        {
          'TableName' => 'Table1',
          'ScanFilter' => {
            'AttributeName2' => {
              'AttributeValueList' => [{'N' => '1'}],
              'ComparisonOperator' => 'GE'
            }
          }
        }
      end

      it 'should not allow count and attributes_to_get simutaneously' do
        expect {
          subject.scan({'Count' => 0, 'AttributesToGet' => ['xx']})
        }.to raise_error(ValidationException, /count/i)
      end

      it 'should only allow limit greater than zero' do
        expect {
          subject.scan(scan.merge('Limit' => 0))
        }.to raise_error(ValidationException, /limit/i)
      end

      it 'should handle basic scan' do
        result = subject.scan(scan)
        result['Count'].should eq(24)

        scan['ScanFilter']['AttributeName2']['ComparisonOperator'] = 'EQ'
        subject.scan(scan)['Count'].should eq(3)
      end

      it 'should return lastevaluated key' do
        scan['Limit'] = 5
        result = subject.scan(scan)
        result['LastEvaluatedKey'].should == {"HashKeyElement"=>{"S"=>"att1"}, "RangeKeyElement"=>{"N"=>"9"}}
        result = subject.scan(scan.merge('Limit' => 100))
        result['LastEvaluatedKey'].should be_nil

        scan.delete('Limit')
        result = subject.scan(scan)
        result['LastEvaluatedKey'].should be_nil
      end
    end
  end
end
