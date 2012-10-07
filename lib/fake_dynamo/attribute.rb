module FakeDynamo
  class Attribute
    attr_accessor :name, :value, :type

    def initialize(name, value, type)
      @name, @value, @type = name, value, type

      if @type == 'B'
        @value = Base64.decode64(value)
      end

      if @type == 'BS'
        @value = value.map { |v| Base64.decode64(v) }
      end

      if ['NS', 'SS', 'BS'].include? @type
        raise ValidationException, 'An AttributeValue may not contain an empty set' if value.empty?
        raise ValidationException, 'Input collection contains duplicates' if value.uniq!
      end

      if ['NS', 'N'].include? @type
        Array(@value).each do |n|
          numeric(n)
        end
      end

      if ['S', 'SS', 'S', 'BS'].include? @type
        Array(value).each do |v|
          raise ValidationException, 'An AttributeValue may not contain an empty string or empty binary' if v == ''
        end
      end

      if name == ''
        raise ValidationException, 'Empty attribute name'
      end
    end

    def numeric(n)
      begin
        Float(n)
      rescue
        raise ValidationException, "The parameter cannot be converted to a numeric value: #{n}"
      end
    end

    def description
      {
        'AttributeName' => name,
        'AttributeType' => type
      }
    end

    def as_hash
      value = if @type == 'B'
                Base64.encode64(@value)
              elsif @type == 'BS'
                @value.map { |v| Base64.encode64(v) }
              else
                @value
              end

      { @name => { @type => value } }
    end

    def ==(attribute)
      @name == attribute.name &&
        @value == attribute.value &&
        @type == attribute.type
    end

    def <=>(other)
      if @type == 'N'
        @value.to_f <=> other.value.to_f
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
