module FakeDynamo
  class Num
    include Comparable
    attr_accessor :internal

    def initialize(value)
      validate!(value)
      @internal = value.to_f
    end

    def validate!(n)
      begin
        Float(n)
      rescue
        raise ValidationException, "The parameter cannot be converted to a numeric value: #{n}"
      end
    end

    def <=>(other)
      @internal <=> other.internal
    end

    def ==(other)
      @internal == other.internal
    end

    def hash
      @internal.hash
    end

    def add(other)
      return Num.new(@internal + other.internal)
    end

    def eql?(other)
      self == other
    end

    def to_s
      if @internal.truncate == @internal
        @internal.truncate.to_s
      else
        @internal.to_s
      end
    end
  end
end
