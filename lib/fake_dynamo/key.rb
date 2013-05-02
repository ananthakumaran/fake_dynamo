module FakeDynamo
  class Key
    include Comparable
    extend Validation

    attr_accessor :primary, :range, :tertiary

    class << self
      def from_data(key_data, key_schema)
        key = Key.new
        validate_key_data(key_data, key_schema)
        key.primary = Attribute.from_hash(key_schema.hash_key.name, key_data[key_schema.hash_key.name])

        if key_schema.range_key
          key.range = Attribute.from_hash(key_schema.range_key.name, key_data[key_schema.range_key.name])
        end
        key
      end

      def from_schema(data, key_schema)
        key = Key.new
        validate_key_schema(data, key_schema)
        key.primary = create_attribute(key_schema.hash_key, data)

        if key_schema.range_key
          key.range = create_attribute(key_schema.range_key, data)
        end
        key
      end

      # local secondary indexes have a three part key:
      # hash key -> primary
      # index range key -> range
      # table range key -> tertiary
      def from_index_schema(data, index_schema, table_schema)
        key = Key.new
        validate_key_schema(data, index_schema)
        validate_key_schema(data, table_schema)
        key.primary = create_attribute(index_schema.hash_key, data)
        key.range = create_attribute(index_schema.range_key, data)
        key.tertiary = create_attribute(table_schema.range_key, data)
        key
      end

      def from_index_item(item, key_schema)
        key = Key.new
        key.primary = item.key.primary
        key.range = item[key_schema.range_key.name]
        key.tertiary = item.key.range
        key
      end

      def create_attribute(key, data)
        name = key.name
        attr = Attribute.from_hash(name, data[name])
        attr
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
        @range == key.range &&
        @tertiary == key.tertiary
    end

    def hash
      primary.hash ^ range.hash ^ tertiary.hash
    end

    def as_hash
      result = @primary.as_hash
      if @range
        result.merge!(@range.as_hash)
      end
      if @tertiary
        result.merge!(@tertiary.as_hash)
      end
      result
    end

    def <=>(other)
      [primary, range, tertiary] <=> [other.primary, other.range, other.tertiary]
    end

  end
end
