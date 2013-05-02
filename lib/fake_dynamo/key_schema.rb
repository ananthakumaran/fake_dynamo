module FakeDynamo
  class KeySchema

    attr_accessor :hash_key, :range_key

    def initialize(key_schema, attribute_definitions)
      extract_values(key_schema, attribute_definitions)
    end

    def description
      description = [{'AttributeName' => hash_key.name, 'KeyType' => 'HASH'}]
      if range_key
        description << {'AttributeName' => range_key.name, 'KeyType' => 'RANGE'}
      end
      description
    end

    def keys
      result = [hash_key.name]
      if range_key
        result << range_key.name
      end
      result
    end

    private
    def extract_values(key_schema, attribute_definitions)
      hash_key_name = find(key_schema, 'KeyType', 'HASH', 'AttributeName')
      hash_key_type = find(attribute_definitions, 'AttributeName', hash_key_name, 'AttributeType')
      @hash_key = Attribute.new(hash_key_name, nil, hash_key_type)
      if range_key_name = find(key_schema, 'KeyType', 'RANGE', 'AttributeName', false)
        range_key_type = find(attribute_definitions, 'AttributeName', range_key_name, 'AttributeType')
        @range_key = Attribute.new(range_key_name, nil, range_key_type)
      end
    end

    def find(list, key, value, pluck, raise_on_error = true)
      if element = list.find { |e| e[key] == value }
        element[pluck]
      elsif raise_on_error
        raise ValidationException, 'Some index key attributes are not defined in AttributeDefinitions'
      end
    end
  end
end
