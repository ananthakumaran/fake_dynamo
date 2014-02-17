$: << File.join(File.dirname(File.dirname(__FILE__)), "lib")

require 'simplecov'
SimpleCov.start if ENV['COVERAGE']

require 'rspec'
require 'rack/test'
require 'fake_dynamo'
require 'pry'
require 'tmpdir'

module Utils
  def self.deep_copy(x)
    Marshal.load(Marshal.dump(x))
  end
end

module FakeDynamo
  class Storage
    def initialize
      init_db(File.join(Dir.tmpdir, 'test_db.fdb'))
      delete_db
    end
  end
end

FakeDynamo::Logger.setup(:debug)
