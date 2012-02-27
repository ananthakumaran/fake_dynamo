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
      if not data
        if spec.include?(:required)
          add_errors("value null at '#{param(attribute, parents)}' failed to satisfy the constraint: Member must not be null")
        end
        return
      end

      spec.each do |constrain|
        case constrain
        when :string
          add_errors("The parameter '#{param(attribute, parents)}' must be a string") unless data.kind_of? String
        when :long
          add_errors("The parameter '#{param(attribute, parents)}' must be a long") unless data.kind_of? Fixnum
        when :integer
          add_errors("The parameter '#{param(attribute, parents)}' must be a integer") unless data.kind_of? Fixnum
        when Hash
          new_parents = parents + [attribute]
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
          when :enum
            enum = constrain[:enum]
            unless enum.include? data
              add_errors("Value '#{data}' at '#{param(attribute, parents)}' failed to satisfy the constraint: Member must satisfy enum values set: #{enum}")
            end
          when :structure
            structure = constrain[:structure]
            structure.each do |attribute, spec|
              validate_spec(attribute, data[attribute], spec, new_parents)
            end
          when :map
            map = constrain[:map]
            raise "#{param(attribute, parents)} must be a Hash" unless data.kind_of? Hash
            data.each do |key, value|
              validate_spec(key, key, map[:key], new_parents)
              validate_spec(key, value, map[:value], new_parents)
            end
          when :list
            raise "#{param(attribute, parents)} must be a Array" unless data.kind_of? Array
            data.each do |element|
              validate_spec(element, element, constrain[:list], new_parents)
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

    def validate_type(value, attribute)
      if attribute.kind_of?(Attribute)
        expected_type = value.keys.first
        if expected_type != attribute.type
          raise ValidationException, "Type mismatch for key #{attribute.name}"
        end
      else
        raise 'Unknown attribute'
      end
    end

    def validate_key_schema(data, key_schema)
      key = data[key_schema.hash_key.name] or raise ValidationException, "Missing the key #{key_schema.hash_key.name} in the item"
      validate_type(key, key_schema.hash_key)

      if key_schema.range_key
        range_key = data[key_schema.range_key.name] or raise ValidationException, "Missing the key #{key_schema.range_key.name} in the item"
        validate_type(range_key, key_schema.range_key)
      end
    end

    def validate_key_data(data, key_schema)
      validate_type(data['HashKeyElement'], key_schema.hash_key)

      if key_schema.range_key
        range_key = data['RangeKeyElement'] or raise ValidationException, "Missing the key RangeKeyElement in the Key"
        validate_type(range_key, key_schema.range_key)
      elsif data['RangeKeyElement']
        raise ValidationException, "RangeKeyElement is not present in the schema"
      end
    end

  end
end
