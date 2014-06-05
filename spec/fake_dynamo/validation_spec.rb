require 'spec_helper'

module FakeDynamo
  class ValidationTest
    include Validation
  end

  describe Validation do
    let(:data) do
      {
        "TableName" => "Table1",
        "AttributeDefinitions" =>
        [{"AttributeName" => "AttributeName1","AttributeType" => "S"},
         {"AttributeName" => "AttributeName2","AttributeType" => "N"}],
        "KeySchema" =>
        [{"AttributeName" => "AttributeName1","KeyType" => "HASH"},
         {"AttributeName" => "AttributeName2","KeyType" => "RANGE"}],
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

    it 'should allow null comparison operator' do
      subject.validate_payload('Scan', {
          'TableName' => 'Table1',
          'ScanFilter' => {
            'age' => { 'ComparisonOperator' => 'NULL' }
          }
        })
    end

    %w[ReadCapacityUnits WriteCapacityUnits].each do |units|
      it "should validate numericality of #{units}" do
        data['ProvisionedThroughput']['ReadCapacityUnits'] = 'xxx'
        expect { subject.validate_payload('CreateTable', data) }.to raise_error(ValidationException, /long/)
      end
    end


    context '#validate_key_data' do
      let(:schema) do
        KeySchema.new(
          [{ 'AttributeName' => 'id', 'KeyType' => 'HASH'}],
          [{"AttributeName" => "id","AttributeType" => "S"}])
      end

      let(:schema_with_range) do
        KeySchema.new(
          [{ 'AttributeName' => 'id', 'KeyType' => 'HASH'},
           { 'AttributeName' => 'time', 'KeyType' => 'RANGE'}],
          [{ 'AttributeName' => 'id', 'AttributeType' => 'S'},
           { 'AttributeName' => 'time', 'AttributeType' => 'N'}])
      end

      it 'should validate the schema' do
        [[{'id' => { 'N' => '1234' }}, schema, /type mismatch/i],
         [{'id' => { 'S' => '1234' }}, schema_with_range, /not match/i],
         [{'id' => { 'S' => '1234' }, 'time' => { 'N' => '1234'}}, schema, /not match/i],
         [{'id' => { 'S' => '1234' }, 'time' => { 'S' => '1234'}}, schema_with_range, /type mismatch/i]
        ].each do |data, schema, message|
          expect do
            subject.validate_key_data(data, schema)
          end.to raise_error(ValidationException, message)
        end
      end
    end

  end
end
