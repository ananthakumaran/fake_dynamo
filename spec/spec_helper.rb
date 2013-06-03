$: << File.join(File.dirname(File.dirname(__FILE__)), "lib")

require 'simplecov'
SimpleCov.start if ENV['COVERAGE']

require 'rspec'
require 'rack/test'
require 'fake_dynamo'
require 'pry'

module Utils
  def self.deep_copy(x)
    Marshal.load(Marshal.dump(x))
  end
end

module FakeDynamo
  class Storage
    def initialize
      delete_db
      init_db
    end
  end
end

FakeDynamo::Storage.db_path = '/tmp/test_db.fdb'
FakeDynamo::Logger.setup(:debug)
