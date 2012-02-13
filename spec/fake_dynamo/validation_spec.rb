require 'spec_helper'

module FakeDynamo
  class ValidationTest
    include Validation
  end

  describe Validation do
    let(:data) do
      {
        "TableName" => "Table1",
        "KeySchema" =>
        {"HashKeyElement" => {"AttributeName" => "AttributeName1","AttributeType" => "S"},
          "RangeKeyElement" => {"AttributeName" => "AttributeName2","AttributeType" => "N"}},
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    subject { ValidationTest.new }

    it 'should validate table name' do
      { '&**' => /pattern/,
        'x'   => /within/,
        'x' * 500 => /within/,
      }.each do |name, msg|
        data['TableName'] = name
        expect { subject.validate_payload('CreateTable', data) }.to raise_error(ValidationException, msg)
      end
    end

    %w[ReadCapacityUnits WriteCapacityUnits].each do |units|
      it "should validate numericality of #{units}" do
        data['ProvisionedThroughput']['ReadCapacityUnits'] = 'xxx'
        expect { subject.validate_payload('CreateTable', data) }.to raise_error(ValidationException, /long/)
      end
    end


  end
end
