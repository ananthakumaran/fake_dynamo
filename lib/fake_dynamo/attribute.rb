module FakeDynamo
  class Attribute
    attr_accessor :name, :value, :type

    def initialize(name, value, type)
      @name, @value, @type = name, value, type

      if ['NS', 'SS'].include? @type
        raise ValidationException, 'Input collection contains duplicates' if value.uniq!
      end

      if ['NS', 'N'].include? @type
        Array(@value).each do |n|
          begin
            Integer(n)
          rescue
            raise ValidationException, "The parameter cannot be converted to a numeric value: #{n}"
          end
        end
      end
    end

    def description
      {
        'AttributeName' => name,
        'AttributeType' => type
      }
    end

    def as_hash
      { @name => { @type => @value } }
    end

    def ==(attribute)
      @name == attribute.name &&
        @value == attribute.value &&
        @type == attribute.type
    end

    def <=>(other)
      if @type == 'N'
        @value.to_i <=> other.value.to_i
      else
        @value <=> other.value
      end
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
