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
  end
end
