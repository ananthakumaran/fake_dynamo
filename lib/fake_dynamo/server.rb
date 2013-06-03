require 'sinatra/base'

module FakeDynamo
  class Server < Sinatra::Base

    set :show_exceptions, false
    set :environment, :production
    set :lock, true

    post '/' do
      status = 200
      content_type 'application/x-amz-json-1.0'
      begin
        data = JSON.parse(request.body.read)
        operation = extract_operation(request.env)
        log.info "operation #{operation}"
        log.info "data"
        log.pp(data)
        response = db.process(operation, data)
        storage.persist(operation, data)
      rescue FakeDynamo::Error => e
        response, status = e.response, e.status
      end
      log.info "response"
      log.pp(response)
      [status, response.to_json]
    end

    delete '/' do
      db.reset
      storage.reset
      {success: true}.to_json
    end

    def db
      DB.instance
    end

    def storage
      Storage.instance
    end

    def log
      Logger.log
    end

    def extract_operation(env)
      if env['HTTP_X_AMZ_TARGET'] =~ /DynamoDB_\d+\.([a-zA-z]+)/
        $1
      else
        raise UnknownOperationException
      end
    end
  end
end
