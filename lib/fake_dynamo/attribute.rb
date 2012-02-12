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

    class << self
      def from_data(data)
        raise ValidationException, 'KeySchema should have HashKeyElement' unless data
        Attribute.new(data['AttributeName'], nil, data['AttributeType'])
      end
    end
  end
end
