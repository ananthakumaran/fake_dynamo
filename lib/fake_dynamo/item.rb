module FakeDynamo
  class Item
    attr_accessor :key, :attributes

    def initialize(data, key_schema)
      @key = Key.new(data, key_schema)

      @attributes = {}
      data.each do |name, value|
        unless key[name]
          @attributes[name] = Value.new(value.keys.first, value.values.first)
        end
      end
    end

    def [](name)
      attributes[name] or key[name]
    end
  end
end
