require 'sinatra/base'

module FakeDynamo
  class Server < Sinatra::Base

    set :show_exceptions, false

    post '/' do
      status = 200
      content_type 'application/x-amz-json-1.0'
      begin
        data = JSON.parse(request.body.read)
        operation = extract_operation(request.env)
        response = db.process(operation, data)
      rescue Error => e
        response, status = e.response, e.status
      end
      [status, response.to_json]
    end

    def db
      @db ||= DB.new
    end

    def extract_operation(env)
      if env['HTTP_x-amz-target'] =~ /DynamoDB_\d+\.([a-zA-z]+)/
        $1
      else
        raise InvalidParameterValueException
      end
    end
  end
end
