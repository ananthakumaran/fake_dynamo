module FakeDynamo
  class Key
    include Validation

    attr_accessor :primary, :range, :schema

    def initialize(data, key_schema)
      @schema = schema
      validate_key_schema(data, key_schema)
      @primary = create_attribute(key_schema.hash_key, data)

      if key_schema.range_key
        @range = create_attribute(key_schema.range_key, data)
      end
    end

    def [](name)
      return @primary if @primary.name == name
      return @range if @range and @range.name == name
      nil
    end

    def eql?(key)
      return false unless key.kind_of? Key

      @primary == key.primary &&
        @range == key.range
    end

    def hash
      primary.hash ^ range.hash
    end

    def data
      result = @primary.data
      if @range
        result.merge!(@range.data)
      end
      result
    end

    private
    def create_attribute(key, data)
      name = key.name
      attr = Attribute.from_hash(name, data[name])
      attr
    end
  end
end
