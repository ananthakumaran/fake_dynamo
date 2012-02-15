module FakeDynamo
  class Attribute
    attr_accessor :name, :value, :type

    def initialize(name, value, type)
      @name, @value, @type = name, value, type
    end

    def description
      {
        'AttributeName' => name,
        'AttributeType' => type
      }
    end

    def data
      { @name => { @type => @value } }
    end

    def ==(attribute)
      @name == attribute.name &&
        @value == attribute.value &&
        @type == attribute.type
    end

    def eql?(attribute)
      return false unless attribute.kind_of? Attribute

      self == attribute
    end

    def hash
      name.hash ^ value.hash ^ type.hash
    end

    class << self
      def from_data(data)
        Attribute.new(data['AttributeName'], nil, data['AttributeType'])
      end

      def from_hash(name, hash)
        Attribute.new(name, hash.values.first, hash.keys.first)
      end
    end
  end
end
