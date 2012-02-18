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
      its(:last_increased_time) { should be_a_kind_of(Fixnum) }
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

      it 'should putitem in the table' do
        subject.put_item({ 'TableName' => 'Table1',
                           'Item' => {
                             'AttributeName1' => { 'S' => "test" },
                             'AttributeName2' => { 'N' => '11' },
                             'AttributeName3' => { 'S' => "another" }
                           }})
        subject.items.size.should == 1
      end

      context 'Expected & ReturnValues' do
        let(:item) do
          { 'TableName' => 'Table1',
            'Item' => {
              'AttributeName1' => { 'S' => "test" },
              'AttributeName2' => { 'N' => '11' },
              'AttributeName3' => { 'S' => "another" }
            }}
        end

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
          subject.put_item(item).should include({ 'ConsumedCapacityUnits' => 1 })
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
        table.put_item({ 'TableName' => 'Table1',
                         'Item' => {
                           'AttributeName1' => { 'S' => "test" },
                           'AttributeName2' => { 'N' => '11' },
                           'AttributeName3' => { 'S' => "another" }
                         }})
        table
      end

      it 'should return empty when the key is not found' do
        response = subject.get_item({'TableName' => 'Table1',
                                      'Key' => {
                                        'HashKeyElement' => { 'S' => 'xxx' },
                                        'RangeKeyElement' => { 'N' => '11' }
                                      }
                                    })
        response.should eq({ 'ConsumedCapacityUnits' => 1})
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
  end
end
