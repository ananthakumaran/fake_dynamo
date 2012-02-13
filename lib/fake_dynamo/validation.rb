require 'yaml'

module FakeDynamo
  module Validation

    extend ActiveSupport::Concern
    include ActiveModel::Validations

    def validate!
      if invalid?
        raise ValidationException, errors.full_messages.join(', ')
      end
    end

    def validate_payload(operation, data)
      validate_operation(operation)
    end

    def validate_operation(operation)
      raise UnknownOperationException, "Unknown operation: #{operation}" unless available_operations.include? operation
    end

    def available_operations
      api_config[:operations].keys
    end

    def api_config
      @api_config ||= YAML.load_file(api_config_path)
    end

    def api_config_path
      File.join File.expand_path(File.dirname(__FILE__)), 'api.yml'
    end

  end
end
