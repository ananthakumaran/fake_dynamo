require 'spec_helper'

module FakeDynamo
  describe Server do
    include Rack::Test::Methods

    let(:data) do
      {
        "TableName" => "Table1",
        "KeySchema" =>
        {"HashKeyElement" => {"AttributeName" => "AttributeName1","AttributeType" => "S"},
          "RangeKeyElement" => {"AttributeName" => "AttributeName2","AttributeType" => "N"}},
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end
    let(:app) { Server.new }
    let(:server) { Server.new! }

    it "should extract_operation" do
      server.extract_operation('HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.CreateTable').should eq('CreateTable')
      expect {
        server.extract_operation('HTTP_X_AMZ_TARGET' => 'FakeDB_20111205.CreateTable')
      }.to raise_error(UnknownOperationException)
    end

    it "should send operation to db" do
      post '/', data.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.CreateTable'
      last_response.should be_ok
    end

    it "should handle error properly" do
      post '/', {'x' => 'y'}.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.CreateTable'
      last_response.should_not be_ok
      last_response.status.should eq(400)
    end

    it "should reset database" do
      post '/', {}.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.ListTables'
      JSON.parse(last_response.body)["TableNames"].size.should == 1

      delete '/'
      last_response.should be_ok

      post '/', {}.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.ListTables'
      JSON.parse(last_response.body)["TableNames"].size.should == 0

      post '/', data.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.CreateTable'
      post '/', {}.to_json, 'HTTP_X_AMZ_TARGET' => 'DynamoDB_20111205.ListTables'
      JSON.parse(last_response.body)["TableNames"].size.should == 1
    end

  end
end
