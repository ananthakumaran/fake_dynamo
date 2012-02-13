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
      subject { DB.new }

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
  end
end
