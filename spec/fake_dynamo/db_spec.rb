require 'spec_helper'

module FakeDynamo
  describe DB do
    let(:data) do
      {
        "TableName" => "Table1",
        "KeySchema" =>
        {"HashKeyElement" => {"AttributeName" => "AttributeName1","AttributeType" => "S"},
          "RangeKeyElement" => {"AttributeName" => "AttributeName2","AttributeType" => "N"}},
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    it 'should not allow to create duplicate tables' do
      subject.create_table(data)
      expect { subject.create_table(data) }.to raise_error(ResourceInUseException, /duplicate/i)
    end

    it 'should fail on unknown operation' do
      expect { subject.process('unknown', data) }.to raise_error(UnknownOperationException, /unknown/i)
    end

    context 'DescribeTable' do
      it 'should describe table' do
        table = subject.create_table(data)
        description = subject.describe_table({'TableName' => 'Table1'})
        description.should include({
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
                            'HashKeyElement' => { 'S' => 'test' },
                            'RangeKeyElement' => { 'N' => '11' }
                          },
                          'AttributesToGet' => ['AttributeName3']
                        })
        subject.process('DeleteItem', {
                          'TableName' => 'Table1',
                          'Key' => {
                            'HashKeyElement' => { 'S' => 'test' },
                            'RangeKeyElement' => { 'N' => '11' }
                          }})
        subject.process('UpdateItem', {
                          'TableName' => 'Table1',
                          'Key' => {
                            'HashKeyElement' => { 'S' => 'test' },
                            'RangeKeyElement' => { 'N' => '11' }
                          },
                          'AttributeUpdates' =>
                          {'AttributeName3' =>
                            {'Value' => {'S' => 'AttributeValue3_New'},
                              'Action' => 'PUT'}
                          },
                          'ReturnValues' => 'ALL_NEW'
                        })
      end
    end
  end
end
