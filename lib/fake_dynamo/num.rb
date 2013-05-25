require 'bigdecimal'

module FakeDynamo
  class Num
    include Comparable
    attr_accessor :internal
    LOW = BigDecimal.new('-.1e126')
    HIGH = BigDecimal.new('.1e126')

    def initialize(value)
      validate!(value)
      @internal = BigDecimal.new(value)
    end

    def validate!(n)
      begin
        Float(n)
      rescue
        raise ValidationException, "The parameter cannot be converted to a numeric value: #{n}"
      end

      b = BigDecimal.new(n)
      if b < LOW
        raise ValidationException, "Number underflow. Attempting to store a number with magnitude smaller than supported range"
      end

      if b > HIGH
        raise ValidationException, "Number overflow. Attempting to store a number with magnitude larger than supported range"
      end

      significant = b.to_s('F').sub('.', '')
        .sub(/^0*/, '').sub(/0*$/, '').size

      if significant > 38
        raise ValidationException, "Attempting to store more than 38 significant digits in a Number"
      end
    end

    def <=>(other)
      @internal <=> other.internal
    end

    def ==(other)
      @internal == other.internal
    end

    def hash
      to_s.hash
    end

    def add(other)
      return Num.new(@internal + other.internal)
    end

    def eql?(other)
      self == other
    end

    def to_s
      @internal.to_s('F').sub(/\.0$/, '')
    end
  end
end
