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
      subject.CreateTable(data)
      expect { subject.CreateTable(data) }.to raise_error(ResourceInUseException, /duplicate/i)
    end

    it 'should fail on invalid operation' do
      expect { subject.process('invalid', data) }.to raise_error(InvalidParameterValueException, /invalid/i)
    end
  end
end
