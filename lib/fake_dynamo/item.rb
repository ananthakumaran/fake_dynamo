module FakeDynamo
  class Item
    attr_accessor :key, :attributes

    def initialize(data, key_schema)
      @key = Key.from_schema(data, key_schema)

      @attributes = {}
      data.each do |name, value|
        unless key[name]
          @attributes[name] = Attribute.from_hash(name, value)
        end
      end
    end

    def [](name)
      attributes[name] or key[name]
    end

    def as_hash
      result = {}
      result.merge!(key.as_hash)
      @attributes.each do |name, attribute|
        result.merge!(attribute.as_hash)
      end
      result
    end
  end
end
