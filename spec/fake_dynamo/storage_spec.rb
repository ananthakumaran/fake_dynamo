# -*- coding: utf-8 -*-
require 'spec_helper'

module FakeDynamo
  describe Storage do

    let(:table) do
      {"TableName" => "User",
        "AttributeDefinitions" =>
        [{"AttributeName" => "id","AttributeType" => "S"}],
        "KeySchema" =>
        [{"AttributeName" => "id","KeyType" => "HASH"}],
        "ProvisionedThroughput" => {"ReadCapacityUnits" => 5,"WriteCapacityUnits" => 10}
      }
    end

    def item(i)
      {'TableName' => 'User',
        'Item' => { 'id' => { 'S' => (i % 100).to_s },
          'name' => { 'S' => "╩tr¥in" },
          'binary' => { 'B' => Base64.encode64("binary") },
          'binary' => { 'BS' => [Base64.encode64("binary")] }}
      }
    end

    it 'compacts and loads db properly' do
      db = DB.instance
      db.tables = {}

      db.process('CreateTable', table)
      subject.persist('CreateTable', table)

      1000.times do |i|
        db.process('PutItem', item(i))
        subject.persist('PutItem', item(i))
      end

      @items = db.tables.values.map { |t| t.items.values.map(&:as_hash) }
      3.times do
        db.tables = {}
        subject.loaded = false
        subject.load_aof
        subject.compacted = false
        subject.compact!
        db.tables.values.map { |t| t.items.values.map(&:as_hash) }.should == @items
      end
    end
  end
end
