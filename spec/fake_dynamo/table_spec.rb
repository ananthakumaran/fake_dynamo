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

    its(:status) { should == 'ACTIVE' }
    its(:creation_date_time) { should_not be_nil }

    it 'should validate table name' do
      { '&**' => /invalid/,
        'x'   => /short/,
        'x' * 500 => /long/,
      }.each do |name, msg|
        data['TableName'] = name
        expect { Table.new(data) }.to raise_error(ValidationException, msg)
      end
    end

    %w[ReadCapacityUnits WriteCapacityUnits].each do |units|
      it "should validate numericality of #{units}" do
        data['ProvisionedThroughput']['ReadCapacityUnits'] = 'xxx'
        expect { Table.new(data) }.to raise_error(ValidationException, /not a number/)
      end
    end

  end
end
