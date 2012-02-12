$: << File.join(File.dirname(File.dirname(__FILE__)), "lib")

require 'simplecov'
SimpleCov.start

require 'rspec'
require 'rack/test'
require 'fake_dynamo'
require 'pry'
