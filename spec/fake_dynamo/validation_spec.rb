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


    context '#validate_key_data' do
      let(:schema) do
        KeySchema.new({'HashKeyElement' => { 'AttributeName' => 'id', 'AttributeType' => 'S'}})
      end

      let(:schema_with_range) do
        KeySchema.new({'HashKeyElement' => { 'AttributeName' => 'id', 'AttributeType' => 'S'},
                        'RangeKeyElement' => { 'AttributeName' => 'time', 'AttributeType' => 'N'}})
      end

      it 'should validate the schema' do
        [[{'HashKeyElement' => { 'N' => '1234' }}, schema, /mismatch/],
         [{'HashKeyElement' => { 'S' => '1234' }}, schema_with_range, /missing.*range/i],
         [{'HashKeyElement' => { 'S' => '1234' }, 'RangeKeyElement' => { 'N' => '1234'}}, schema, /not present/],
         [{'HashKeyElement' => { 'S' => '1234' }, 'RangeKeyElement' => { 'S' => '1234'}}, schema_with_range, /mismatch/]
        ].each do |data, schema, message|
          expect do
            subject.validate_key_data(data, schema)
          end.to raise_error(ValidationException, message)
        end
      end
    end

  end
end
