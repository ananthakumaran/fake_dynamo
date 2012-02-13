require 'yaml'

module FakeDynamo
  module Validation

    def validate!(&block)
      @api_errors = []
      yield
      unless @api_errors.empty?
        plural = @api_errors.size == 1 ? '' : 's'
        message = "#{@api_errors.size} error#{plural} detected: #{@api_errors.join('; ')}"
        raise ValidationException, message
      end
    end


    def add_errors(message)
      @api_errors << message
    end

    def validate_payload(operation, data)
      validate! do
        validate_operation(operation)
        validate_input(operation, data)
      end
    end

    def validate_operation(operation)
      raise UnknownOperationException, "Unknown operation: #{operation}" unless available_operations.include? operation
    end

    def available_operations
      api_config[:operations].keys
    end

    def validate_input(operation, data)
      api_input_spec(operation).each do |attribute, spec|
        validate_spec(attribute, data[attribute], spec, [])
      end
    end

    def validate_spec(attribute, data, spec, parents)
      if spec.include?(:required) and not data
        add_errors("value null at '#{param(attribute, parents)}' failed to satisfy the constraint: Member must not be null")
        return
      end

      spec.each do |constrain|
        case constrain
        when :string
          add_errors("The parameter '#{param(attribute, parents)}' must be a string") unless data.kind_of? String
        when :long
          add_errors("The parameter '#{param(attribute, parents)}' must be a long") unless data.kind_of? Fixnum
        when Hash
          case constrain.keys.first
          when :pattern
            pattern = constrain[:pattern]
            unless data =~ pattern
              add_errors("The parameter '#{param(attribute, parents)}' should match the pattern #{pattern}")
            end
          when :within
            range = constrain[:within]
            unless range.include? data.size
              add_errors("The parameter '#{param(attribute, parents)}' value '#{data}' should be within #{range} characters")
            end
          when :structure
            structure = constrain[:structure]
            new_parents = parents + [attribute]
            structure.each do |attribute, spec|
              validate_spec(attribute, data[attribute], spec, new_parents)
            end
          else
            raise "Unhandled constraint #{constrain}"
          end
        when :required
          # handled earlier
        else
          raise "Unhandled constraint #{constrain}"
        end
      end
    end

    def param(attribute, parents)
      (parents + [attribute]).join('.')
    end

    def api_input_spec(operation)
      api_config[:operations][operation][:input]
    end

    def api_config
      @api_config ||= YAML.load_file(api_config_path)
    end

    def api_config_path
      File.join File.expand_path(File.dirname(__FILE__)), 'api.yml'
    end

  end
end
