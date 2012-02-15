module FakeDynamo
  class Item
    attr_accessor :key, :attributes

    def initialize(data, key_schema)
      @key = Key.new(data, key_schema)

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

    def data
      result = { 'Attributes' => {} }
      result['Attributes'].merge!(key.data)
      @attributes.each do |name, attribute|
        result['Attributes'].merge!(attribute.data)
      end
      result
    end
  end
end
