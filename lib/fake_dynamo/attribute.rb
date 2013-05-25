module FakeDynamo
  class Attribute
    attr_accessor :name, :value, :type

    def initialize(name, value, type)
      @name, @type = name, type
      validate_name!
      return unless value

      @value = decode(value)
      validate_value!
    end

    def validate_name!
      if name == ''
        raise ValidationException, 'Empty attribute name'
      end
    end

    def validate_value!
      if ['NS', 'SS', 'BS'].include? @type
        raise ValidationException, 'An AttributeValue may not contain an empty set' if @value.empty?
        raise ValidationException, 'Input collection contains duplicates' if value.uniq!
      end

      if ['S', 'SS', 'S', 'BS'].include? @type
        Array(@value).each do |v|
          raise ValidationException, 'An AttributeValue may not contain an empty string or empty binary' if v == ''
        end
      end
    end

    def description
      {
        'AttributeName' => name,
        'AttributeType' => type
      }
    end

    def decode(value)
      case @type
      when 'B' then Base64.decode64(value)
      when 'BS' then value.map { |v| Base64.decode64(v) }
      when 'N' then Num.new(value)
      when 'NS' then value.map { |v| Num.new(v) }
      else value
      end
    end

    def encode(value)
      case @type
      when 'B' then Base64.encode64(value)
      when 'BS' then value.map { |v| Base64.encode64(v) }
      when 'N' then value.to_s
      when 'NS' then value.map(&:to_s)
      else value
      end
    end

    def as_hash
      { @name => { @type => encode(@value) } }
    end

    def ==(attribute)
      @name == attribute.name &&
        @type == attribute.type &&
        @value == attribute.value
    end

    def <=>(other)
      @value <=> other.value
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
